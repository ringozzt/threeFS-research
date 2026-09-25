# DeepSeek 3FS（Fire-Flyer File System）系统调研报告

> 调研对象：[deepseek-ai/3FS](https://github.com/deepseek-ai/3FS)
> 调研日期：2026-09-26
> 目标：系统理解 3FS 的设计动机、架构、核心机制、源码结构与适用场景，形成可复用的学习笔记。

---

## 目录

1. [一句话概括](#1-一句话概括)
2. [项目背景与定位](#2-项目背景与定位)
3. [整体架构](#3-整体架构)
4. [核心技术深入](#4-核心技术深入)
5. [性能表现](#5-性能表现)
6. [部署与运维](#6-部署与运维)
7. [源码结构导读](#7-源码结构导读)
8. [与同类系统对比](#8-与同类系统对比)
9. [学习路径建议](#9-学习路径建议)
10. [参考资料](#10-参考资料)

---

## 1. 一句话概括

**3FS 是 DeepSeek 开源的、面向 AI 训练与推理负载的高性能分布式文件系统**：它把成千上万块 NVMe SSD 和 RDMA 网络带宽聚合成一个全局共享存储池，对外提供熟悉的 POSIX 文件接口，并用 **CRAQ 链式复制 + FoundationDB 事务元数据 + RDMA 零拷贝** 这套组合，在强一致性前提下做到了单机 read 吞吐接近网络/SSD 线性扩展（公开实测聚合读 **6.6 TiB/s**）。

它不是"又一个对象存储"，而是专门为下面四类 AI 工作负载设计的：

| 场景 | 3FS 解决的问题 |
|---|---|
| 数据准备（Data Pipeline） | 把分析管道产出的海量中间结果按目录层级高效组织，支持原子 rename、递归 delete |
| DataLoader | 跨计算节点随机读取训练样本，省去预取/shuffle |
| 大规模 Checkpoint | 高吞吐并行写 checkpoint |
| 推理 KVCache | 用 SSD 做 DRAM 的大容量低成本替代，峰值读 40 GiB/s |

---

## 2. 项目背景与定位

### 2.1 它在 DeepSeek 技术栈中的位置

3FS 是 DeepSeek **Fire-Flyer AI-HPC** 体系的存储底座。根据论文 *Fire-Flyer AI-HPC: A Cost-Effective Software-Hardware Co-Design for Deep Learning*（[arXiv:2408.14158](https://arxiv.org/abs/2408.14158)），Fire-Flyer AI-HPC 由三部分组成：

- **HAI Platform**：软硬件协同的训练平台（较早开源）；
- **3FS**：本项目，分布式文件系统（2025 年 2 月开源）；
- **HaiScale**：尚未开源的调度/规模编排层。

DeepSeek 在后续公开的 DSec（Agent 训练沙箱基础设施）等论文里，也直接把镜像和沙箱数据放在 3FS 上按需加载——说明它已经是 DeepSeek 内部训练/推理基础设施的**事实存储层**。

### 2.2 为什么需要新做一个 FS，而不是用 HDFS/Ceph/JuiceFS/Lustre？

设计文档里给出的几条非常关键的判断：

1. **AI 负载的瓶颈是带宽，不是容量**。万卡训练时，DataLoader 要同时从上千个节点拉数据，传统 HDFS 类系统的 NameNode、RPC、三副本写路径都会成为瓶颈。
2. **SSD + RDMA 已经普及，但软件没跟上**。单台机器十几块 14TB NVMe、双 200/400Gbps IB 网卡，需要一个能"locality-oblivious"地把这些带宽拼起来的存储层。
3. **对象存储不够用**。内部应用大量依赖：
   - 原子地 `mv` 整个临时目录到正式位置；
   - 递归删除海量小文件；
   - 硬链接/软链接做轻量快照。
   这些是 POSIX 目录语义天然提供的，对象存储要靠客户端模拟，很难做对。
4. **FUSE 有硬上限**。Linux FUSE 内核队列是一把自旋锁，实测大约只能跑 **40 万 IOPS（4KiB 读）**，再高并发就打满锁；且 Linux 5.x FUSE 不支持同文件并发写。所以不能简单地"在 FUSE 上挂一个网络 FS"就完事。

3FS 的回答是：**文件接口照给（POSIX/FUSE），但关键路径绕开内核栈**——提供一个 io_uring 风格的异步零拷贝 native client（USRBIO），让性能敏感应用直接绕过 FUSE。

---

## 3. 整体架构

### 3.1 四大组件

```
                         ┌──────────────────────────────┐
                         │   Client (FUSE / USRBIO)    │
                         │  计算节点上挂载 / 原生集成    │
                         └──────┬───────────┬───────────┘
                                │ meta RPC  │ data RPC (RDMA)
                ┌───────────────▼──┐     ┌─▼──────────────────────┐
                │  mgmtd (集群管理) │     │  Storage Service × N   │
                │  - 选主         │◄───►│  - 每节点几块本地 SSD    │
                │  - 心跳/ membership│  HB │  - 实现 CRAQ 链复制      │
                │  - 分发 chain table│    │  - chunk engine(RocksDB)│
                └───────┬─────────┘     └─────────────────────────┘
                        │ 心跳
                ┌───────▼─────────┐
                │  Meta Service ×N │
                │  - 无状态        │     ┌─────────────────────┐
                │  - POSIX 语义    │────►│  FoundationDB (KV)  │
                │  - 事务/重试     │     │  存全部 inode/dentry │
                └─────────────────┘     └─────────────────────┘
```

| 组件 | 进程名 | 职责 | 状态 |
|---|---|---|---|
| **mgmtd** | `mgmtd_main` | 集群成员管理、选主、维护并广播 chain table、故障检测 | 多副本选主 |
| **Meta Service** | `meta_main` | 文件/目录/inode 操作，POSIX 语义，事务写 FoundationDB | **无状态**，可任意扩展 |
| **Storage Service** | `storage_main` | 管理本地 SSD，提供 chunk 接口，实现 CRAQ 复制 | 有状态，按 SSD 划分 target |
| **Client** | `hf3fs_fuse_main` + native lib | FUSE 挂载 + USRBIO 异步零拷贝 API | 无状态 |

所有组件之间跑在 **RDMA 网络**（InfiniBand 或 RoCE）上；mgmtd 自己的高可用元数据可以复用 FoundationDB（生产环境就是这么干的，避免再依赖 ZooKeeper/etcd）。

### 3.2 一次文件读的数据流

1. 应用 `open("/mnt/3fs/dataset/x.parquet")` → FUSE daemon 转成 meta RPC；
2. Meta Service 在 FoundationDB 里查出 inode：文件长度、chunk size、stripe size、chain table 范围、shuffle seed；
3. Client 把这些布局信息缓存下来，**自己就能算出 chunk ID 和它所在的 chain**，后续数据请求完全不再经过 meta；
4. 数据请求走 RDMA，直接发到 chain 上任意一个 serving target（read-any）；
5. 写请求走 chain 的 head，沿链传播到 tail 才 ACK。

> 关键设计：**meta 在数据关键路径上只出现一次（open）**，之后 client 是自治的。这是高吞吐的前提。

---

## 4. 核心技术深入

### 4.1 元数据：无状态 Meta + FoundationDB 事务 KV

3FS 把所有文件元数据都存在 FoundationDB（一个支持 Serializable Snapshot Isolation 的分布式事务 KV）里，Meta Service 本身**不存状态**。

**两种核心 key 结构：**

| key 前缀 | 组成 | value |
|---|---|---|
| `INOD<8B inode_id LE>` | 全局单调递增 64 位 inode id（小端序打散到多个 FDB shard） | 文件：长度/chunk size/chain table 范围/shuffle seed；目录：父 inode、默认布局；软链：目标路径 |
| `DENT<parent_inode><name>` | 父 inode + 名字 | 目标 inode id + 类型 |

目录项在 key 上自然连续，`listdir` 就是一次 FDB range scan。

**操作怎么落到事务上：**

- 只读事务：`fstat / lookup / listdir`；
- 读写事务：`create / link / unlink / rename`。
- 并发冲突时 FDB 自动 abort-retry，多个 Meta Service 可以并行处理请求而不破坏一致性。

**几个有意思的工程权衡：**

- **不跟踪只读 fd**：训练任务启动时会打开海量文件，维护所有 fd 代价太大；只对"写打开"的文件维护 session，删写打开的文件时延迟到所有写 fd 关闭才真正回收 chunk。
- **文件长度是最终一致的**：client 每 5 秒上报一次最大写位置；`close/fsync` 时才向 storage 反查最后一个 chunk 的真实长度。
- **小文件长度更新优化**：生产环境 stripe size = 200，但小文件用不到那么多 chain。inode 里记一个"已用 chain 数"提示（初始 16，翻倍增长），避免每次更新长度都去问 200 个 chain。
- **目录 rename 防环**：靠父 inode 字段，`mv dir_a/dir_b dir_c/` 时向上查 `dir_c` 的祖先，确认不是 `dir_b` 的后代。

### 4.2 数据放置：Chain Table 与 stripe

一个文件被切成固定大小的 chunk，每个 chunk 在一条 **replication chain** 上放多副本。

- 每个 SSD 上切出多个 **target**（逻辑存储单元），不同 target 加入不同 chain；
- 多条 chain 组成一张 **chain table**，按目录指定；
- 创建文件时，meta 从 chain table 里按 stripe size 挑连续 chain，再用随机 seed shuffle，保证数据均匀；
- chain 带 version 号，target 上下线就 bump version，请求里带旧 version 会被拒绝。

可以为不同业务建不同 chain table（离线批量 vs 在线服务），两张表用**互斥的 SSD/节点**，互不抢带宽。

### 4.3 一致性协议：CRAQ（Chain Replication with Apportioned Queries）

这是 3FS 存储层的灵魂。CRAQ 是为读多写少场景设计的链式复制：

- **写**：发给 head target，沿 chain 单向传播；只有到达 **tail** 并提交后才 ACK 回 client；
- **读**：chain 上**任意副本都能答**（read-any），把读流量打散到所有副本的 SSD/RDMA 上——这是全闪阵列把读带宽拉满的关键。

**写路径 6 步（design_notes.md 原文）：**

1. target 检查请求里的 chain version 是否最新，不匹配直接拒；
2. 用 **RDMA Read** 主动去 client/前驱节点 pull 写数据（不是对方推过来）；
3. 拿到数据后从 lock manager 取 chunk 锁，同 chunk 写串行化；
4. 读已提交版本 → 改 → 存成 **pending 版本**（每个 target 同时持有 committed `v` 和 pending `v+1`）；
5. 如果是 tail：原子地把 pending 提升为 committed，向前驱 ACK；否则把写转发给后继；
6. ACK 沿链反向传回，沿途每个节点把 pending 提升为 committed，放锁。

**与原版 CRAQ 的一个差异**：3FS 实现里，当一个 target 同时有 committed 和 pending 版本时，**不**向 tail 发版本查询，而是直接回一个特殊状态码给 client；client 可以等一会重试，或显式发 relaxed read 拿 pending 版本。这样省掉了每次读都要跨链问 tail 的开销。

**故障处理**：假设链 `A→B→C`，写刚到 A，B 挂了。mgmtd 检测到后把 B 移到链尾并广播新 chain table。A 收到后把写转发给新后继 C；C 可能还没拿到新表会拒绝，但 A 会一直重试，直到 C 收到新表接受请求。这是一个最终收敛的协议。

### 4.4 故障恢复：平衡的重定向流量

一个朴素 chain table 在节点故障时会出问题：假设链是 `A-B-C`、`D-E-F`…，A 挂了，A 的读流量全压到 B、C，B/C 瞬间饱和，而换盘同步要好几小时，期间整集群读性能塌掉。

3FS 的做法是把 chain table 构造成**平衡不完全块设计（Balanced Incomplete Block Design, BIBD）**：让 A 和其他每块 SSD 都出现在不同链里。A 一旦挂掉，它原本的读流量会被均摊到其余所有 SSD，每块只多承担 1/(N-1)。最优解用整数规划求解。

### 4.5 故障检测与 target 状态机

mgmtd 靠心跳（lease）做 fail-stop 检测：

- T 秒收不到心跳就宣告服务故障；
- 服务自己 T/2 秒联系不上 mgmtd 就主动退出（防脑裂）。

每个 storage target 有两套状态：

**Public state**（存在 chain table 里，广播给所有 client/storage）：

| 状态 | 可读 | 可写 | 说明 |
|---|:-:|:-:|---|
| serving | ✓ | ✓ | 正常 |
| syncing | ✗ | ✓ | 数据恢复中 |
| waiting | ✗ | ✗ | 恢复还没开始 |
| lastsrv | ✗ | ✗ | 最后一个 serving target 挂了 |
| offline | ✗ | ✗ | 服务挂/介质故障 |

**Local state**（只在 mgmtd 内存里）：up-to-date / online / offline，作为 public state 迁移的触发事件。

mgmtd 周期性扫每条 chain，按一张状态迁移表把 local state 翻译成新的 public state；target offline 就移到链尾；如果某个 target 落到 lastsrv/offline，对应 storage 进程会立刻自杀（防止网络分区下继续服务）。

### 4.6 数据恢复（重建副本）

恢复过程和正常服务**重叠**进行：

1. 恢复中的服务先不拉心跳，直到 mgmtd 把它所有 target 标记成 offline（保证都走恢复路径）；
2. 恢复期间写进来的请求都被当成 **full-chunk-replace 写**：前驱节点把自己整 chunk 直接覆盖给它；
3. 恢复前，前驱先发 `dump-chunkmeta`，双方交换各自本地的 chunk 元数据（chunk id、chain version、committed/pending 版本号）；
4. 按一组规则 diff：本地有远端没有 → 传；远端有本地没有 → 删；chain version 本地新 → 传；version 相同但 committed/pending 对不上 → 传；
5. 传的时候对每个 chunk 取锁、读版本、发 full-chunk-replace、放锁；
6. 传完发 `sync-done`，恢复方在下次心跳里把 local state 置为 up-to-date，mgmtd 把 public state 切回 serving。

### 4.7 Chunk Engine：本地 SSD 上的存储引擎

每个 storage node 上，chunk engine 是这样组织的：

- **持久层**：固定数量的数据文件 + 一个 RocksDB 实例（存 chunk 元数据和系统信息）；
- **内存层**：chunk 元数据 hashmap cache（O(1) get）、chunk allocator。

接口：`open/close / get / update / commit`。`update` 是 **COW（copy-on-write）**——改数据前先分配新 chunk，旧 chunk 在所有 handle 释放前继续可读；`commit` 用 RocksDB write batch 原子提交。

**块分配器细节**：物理块大小从 64KiB 到 64MiB 共 11 档（2 的幂），每档一个资源池、256 个物理文件，用 bitmap 在内存里管空闲；回收只清 bitmap，不真正释放空间，下次优先复用；不够了就 `fallocate()` 一段连续空间再切成 256 块——减少碎片。append 写有优化：直接在原块尾部追加，不重写整块。

### 4.8 客户端：FUSE + USRBIO 异步零拷贝 API

FUSE 走标准路径（open/close/stat 等元数据和不挑剔性能的应用），但数据面提供 **USRBIO**（User Space Ring Based IO）：

- **Iov**：用户进程和 FUSE daemon 之间的一大块共享内存，零拷贝读写，IB 内存注册由 FUSE 进程管理；
- **Ior**：一小块共享环形队列，用法像 Linux `io_uring`——用户入队请求，FUSE daemon 出队批量处理；
- fd 注册：应用 `open()` 拿到普通 fd 后，通过 USRBAPI 把 fd 注册进 native client；
- 多线程应用建议每线程一个 Ior（共享 ring 要加锁）；
- 内部多线程从多个 Ior 拉请求、**batch 成大 RPC** 发给 storage，摊掉小读的 RPC 开销。

典型调用（来自 `src/lib/api/UsrbIo.md`）：

```c
struct hf3fs_ior ior;
hf3fs_iorcreate4(&ior, "/hf3fs/mnt", /*entries*/1024,
                 /*for_read*/true, /*io_depth*/0, /*timeout*/0,
                 /*numa*/-1, /*flags*/0);
// ... enqueue read/write requests into ior, data lives in iov ...
hf3fs_iordestroy(&ior);
```

这样设计的好处：元数据仍走 FUSE/POSIX，迁移老代码几乎零成本；性能敏感路径绕开内核 FUSE 队列那把自旋锁和两次内存拷贝。

### 4.9 形式化验证：P specs

3FS 在 `specs/` 下用 Microsoft 的 **P 语言**（异步状态机建模/验证框架）写了两套规范：

- `DataStorage`：对 CRAQ 实现做模型检查，覆盖单/多 client 写、节点故障、不可靠故障检测器、短链/长链等 12 个场景，全部通过；
- `RDMASocket`：验证 RDMA socket 的 pingpong/oneway/twoway 语义。

这在分布式存储开源项目里不多见，说明作者对协议正确性是认真对待的。

---

## 5. 性能表现

来自官方 README 的三组公开数据：

| 指标 | 集群规模 | 结果 |
|---|---|---|
| **大文件读吞吐** | 180 存储节点 × (2×200Gbps IB + 16×14TiB NVMe)，500+ 客户端 | 聚合读 **~6.6 TiB/s**（背景还有训练流量） |
| **GraySort 排序** | 25 存储节点（2 NUMA/node，2×400Gbps）+ 50 计算节点 | 排 **110.5 TiB** 数据，8192 分区，用时 **30min14s**，平均 **3.66 TiB/min** |
| **KVCache 读** | 客户端 1×400Gbps NIC | 峰值读 **40 GiB/s**，并展示了 GC 剔除垃圾对象的 IOPS |

benchmark 工具：`benchmarks/fio_usrbio`（基于 fio 的 USRBIO 引擎）和 `benchmarks/storage_bench`。

---

## 6. 部署与运维

官方 `deploy/README.md` 给了一个 6 节点最小集群示例：

| 节点 | 角色 | 配置 |
|---|---|---|
| meta（192.168.1.1） | mgmtd + meta + monitor + FoundationDB + ClickHouse | 128GB 内存 |
| storage1~5（.2~.6） | storage | 512GB 内存，14TB×16 NVMe，RoCE |

**组件清单**：

- `monitor_collector_main`：指标收集，上报到 ClickHouse（建表 SQL 在 `deploy/sql/3fs-monitor.sql`）；
- `admin_cli`：管理 CLI，连所有节点；
- `mgmtd_main` / `meta_main` / `storage_main`：核心服务；
- `hf3fs_fuse_main`：客户端挂载进程。

**依赖**：

- libfuse ≥ 3.16.1；
- FoundationDB ≥ 7.1（生产建议独立节点）；
- Rust ≥ 1.75（推荐 1.85）；
- 一堆 C++ 依赖：libuv、lz4、lzma、double-conversion、libaio、gflags、glog、gtest、boost、openssl、numactl 等；
- 支持 Ubuntu 20.04/22.04、openEuler 2403sp1、OpenCloudOS 9 / TencentOS 4。

配置都是 TOML，按"主配置 + launcher + app"三层拆分（例如 `mgmtd_main.toml` / `mgmtd_main_launcher.toml` / `mgmtd_main_app.toml`），systemd unit 在 `deploy/systemd/`。

---

## 7. 源码结构导读

仓库根目录速览：

```
3FS/
├── src/                    # C++ 主代码（~830 文件，~10 万行）
│   ├── mgmtd/              # 集群管理：选主、chain table、故障检测、状态机
│   ├── meta/               # 元数据服务：POSIX 语义、FDB 事务、session 管理
│   ├── storage/            # 存储服务：
│   │   ├── chunk_engine/   #   - RocksDB 元数据 + COW + 块分配器
│   │   ├── aio/            #   - 异步 IO
│   │   ├── sync/           #   - 副本恢复（dump-chunkmeta / full-chunk-replace）
│   │   ├── update/         #   - CRAQ 写路径
│   │   └── store/          #   - 本地 chunk 存储
│   ├── client/             # FUSE client、meta client、storage client、trash cleaner
│   ├── common/             # 公共库：app/ net/ kv/ serde/ logging/ monitor/ utils
│   ├── fdb/                # FoundationDB 封装
│   ├── kv/                 # KV 抽象
│   ├── fuse/               # FUSE 适配层
│   ├── lib/api/            # USRBIO C API（UsrbIo.md 在这里）
│   ├── lib/rs/             # Rust 绑定
│   ├── migration/          # 工具/迁移
│   ├── monitor_collector/  # 指标收集
│   └── tools/              # 运维工具
├── hf3fs/  hf3fs_fuse/     # Python 侧封装（setup.py、fuse.py）
├── hf3fs_utils/            # Python 工具
├── specs/                  # P 语言形式化模型（CRAQ + RDMASocket）
├── configs/                # 所有 TOML 配置模板
├── deploy/                 # 部署文档、systemd、SQL、data_placement 工具
├── benchmarks/             # fio_usrbio、storage_bench
├── dockerfile/             # 容器构建
├── patches/                # 对第三方 submodule 的 patch
└── third_party/            # submodule（包括 fdb client、rdma 相关等）
```

**推荐阅读顺序**（想真正读懂代码的话）：

1. `docs/design_notes.md`（已经把设计讲透了，先读 2 遍）；
2. `src/mgmtd/` → 看 chain table 怎么维护、状态迁移表怎么实现；
3. `src/storage/update/` + `src/storage/sync/` → CRAQ 写路径和恢复路径；
4. `src/meta/` → FDB key 怎么编码、事务怎么用；
5. `src/lib/api/UsrbIo.md` + `src/client/` → native client 和 FUSE 集成；
6. `specs/DataStorage/` → 对照 P 模型看协议边界 case。

构建注意：clone 后必须 `git submodule update --init --recursive && ./patches/apply.sh` 才能编译。

---

## 8. 与同类系统对比

| 维度 | 3FS | JuiceFS | HDFS | CephFS | Lustre/GPFS |
|---|---|---|---|---|---|
| 定位 | AI 训练/推理高吞吐共享存储 | 通用云原生 FS | 大数据离线 | 通用分布式块/对象/文件 | HPC 并行 FS |
| 元数据后端 | FoundationDB（事务 KV） | 任意 Redis/DB/TiKV | NameNode 内存 | MDS + RADOS | 自有 MDS |
| 数据副本协议 | **CRAQ 链复制，读任意副本** | 主从/EC，读写都走固定副本 | 三副本 pipeline | 主从+EC | 条带 + OST |
| 网络 | **RDMA 一等公民** | TCP 为主 | TCP | TCP/RADOS | RDMA/InfiniBand |
| 介质 | 全闪 NVMe 设计 | 盘/对象存储都行 | 盘 | 盘 | 盘/全闪 |
| 客户端 | FUSE + **USRBIO 零拷贝 native API** | FUSE/S3/HDFS | HDFS API | FUSE/Kernel | Kernel 模块 |
| 一致性 | 强一致（CRAQ + FDB SSI） | 强一致 | 强一致（单 NN） | 强一致 | 强一致 |
| 读吞吐扩展 | 线性叠加所有副本带宽 | 受主副本/EC 限制 | 受 NN 和单副本限制 | 受 MDS/主副本限制 | 优秀但成本高 |
| 小文件/随机读 | 好（USRBIO batch） | 一般 | 差（NN 瓶颈） | 一般 | 好但贵 |

**JuiceFS 官方对比页**（[juicefs.com/docs/community/comparison/juicefs_vs_3fs](https://juicefs.com/docs/community/comparison/juicefs_vs_3fs/)）也提到：3FS 用本地 NVMe + CRAQ，写延迟因链上串行传播而更高，但读吞吐优先——这正是 AI 场景要的。

**一句话总结**：3FS 不是通用 FS，它是"为万卡训练和大模型推理专门做的全闪 RDDA 存储"，把一致性、元数据扩展性、读带宽这三件事用 CRAQ + FDB + RDMA 这三个组件解耦地做到了极致；代价是部署门槛高（必须 RDMA、必须 NVMe、依赖 FoundationDB）、容量成本不便宜、不适合冷数据。

---

## 9. 学习路径建议

如果目标是"系统掌握"而不是"跑起来玩一下"，建议按下面四步走：

### 阶段 1：建立直觉（0.5 天）
- 读这篇报告 + `docs/design_notes.md`；
- 看 DeepSeek 的 Fire-Flyer AI-HPC 论文（arXiv:2408.14158）里关于 3FS 的章节；
- 能画出四大组件和一次读/写的数据流。

### 阶段 2：跑起来（1–2 天）
- 按 `deploy/README.md` 在 6 台 VM/物理机上搭最小集群（没有 RDMA 也能跑 TCP 模式学架构）；
- 用 `fio_usrbio` 跑一遍吞吐 benchmark；
- 观察 `monitor_collector` 上报到 ClickHouse 的指标。

### 阶段 3：读核心代码（3–5 天）
按上面 [§7 源码导读](#7-源码结构导读) 的顺序，重点吃透：
- CRAQ 写路径在 `src/storage/update/`；
- 副本恢复在 `src/storage/sync/`；
- chain table 状态机在 `src/mgmtd/`；
- FDB 事务封装在 `src/fdb/` 和 `src/meta/`。

### 阶段 4：对照形式化模型（1 天）
- 读 `specs/DataStorage/` 里的 P 模型，把代码里的分支和模型里的 case 一一对应；
- 重点看：节点在写路径中途挂掉、不可靠故障检测器、短链/长链这些 case 是怎么被模型覆盖的。

### 可深入的延伸课题
- BIBD 是怎么用整数规划解出来的（`deploy/data_placement/`）；
- USRBIO 的 batch 策略和 io_uring 的差异；
- 3FS 的 KVCache 用法（DeepSeek 推理场景怎么把 SSD 当 DRAM 扩展）；
- 和 smallpond（DeepSeek 的 Rust 数据处理框架）怎么配合。

---

## 10. 参考资料

- 仓库：<https://github.com/deepseek-ai/3FS>
- 设计文档：<https://github.com/deepseek-ai/3FS/blob/main/docs/design_notes.md>
- 部署指南：<https://github.com/deepseek-ai/3FS/blob/main/deploy/README.md>
- USRBIO API：<https://github.com/deepseek-ai/3FS/blob/main/src/lib/api/UsrbIo.md>
- 论文：*Fire-Flyer AI-HPC: A Cost-Effective Software-Hardware Co-Design for Deep Learning*，arXiv:2408.14158 — <https://arxiv.org/abs/2408.14158>
- JuiceFS 官方对比：<https://juicefs.com/docs/community/comparison/juicefs_vs_3fs/>
- CRAQ 原始论文：*Chain Replication with Apportioned Queries for Improving Fast Modern Microsecond-scale Storage*（2015）
- FoundationDB 文档：<https://apple.github.io/foundationdb/>
- P 语言（形式化验证）：<https://p-org.github.io/P/>

---

*本报告基于 2026-09-26 当天 clone 的 main 分支（commit 22fca04）整理，后续上游更新请以官方仓库为准。*
