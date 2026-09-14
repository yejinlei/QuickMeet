# QuickMeet 分布式集群设计、部署与扩容

对应 Issue **YEJ-95 / QM-006**，父 Epic **YEJ-89**，上游依赖 **QM-005**（房间模型与权限）。

本文覆盖三件事：**集群怎么跑起来**（部署）、**节点怎么加和减**（扩容）、
**出问题怎么排查**（运维）。代码在 `crates/qm-cluster/`。

---

## 1. 架构

```
                        ┌──────────────────────────────┐
                        │        NATS (core)            │
                        │  qm.<cluster_id>.hb.*         │
                        │  qm.<cluster_id>.room.*       │
                        │  qm.<cluster_id>.room.snapshot│
                        │  qm.<cluster_id>.route.new    │
                        │  qm.<cluster_id>.migrate.req.*│
                        │  qm.<cluster_id>.migrate.done │
                        └──┬─────────┬─────────┬───────┘
                           │         │         │
              ┌────────────┘         │         └────────────┐
              ▼                      ▼                       ▼
      ┌───────────────┐    ┌───────────────┐        ┌───────────────┐
      │  node-1 (full)│    │  node-2 (full)│        │ listener-1    │
      │  8080 / 8081  │    │  8082 / 18081 │        │  (listener)   │
      │  rooms: {…}   │    │  rooms: {…}   │        │  只转发旁听流  │
      └───────────────┘    └───────────────┘        └───────────────┘
```

### 1.1 为什么是 core NATS，不用 JetStream

房间状态是**短生命周期的实时数据**：会议结束就作废，重启后不需要恢复历史。
所以本方案只用 core NATS 的 pub/sub 与 request/reply，不启用 JetStream / KV：

- 少一个持久化队列要维护的故障模式（磁盘满、消费组积压）；
- NATS 容器本身无状态，`docker-compose up -d` 一次就起来；
- 节点掉线后的状态恢复靠**重新订阅 + 一次全量快照**，不靠消息重放。

代价是：NATS 本身宕机时集群会退化为单机模式（各节点仍能开会，但不再互相感知）。
见 §6.2。

### 1.2 三个分层

| 模块 | 职责 | 是否有 IO |
| --- | --- | --- |
| `state.rs` | 房间/节点状态模型、最低负载调度、健康判定、迁移目标选择 | 否（纯函数） |
| `bus.rs` | NATS 传输：subject 规划、pub/sub、request/reply | 是 |
| `cluster.rs` | 编排：心跳、订阅循环、故障迁移、对外 API | 是 |

把决策做成纯函数是有意的：调度逻辑、判死逻辑、迁移目标选择**全部可以在
没有 NATS server 的情况下单测**，验收逻辑逐条断言而不是靠肉眼评审。
这也是为什么 `cargo test -p qm-cluster` 能在这台没有 NATS 的开发机上跑绿。

### 1.3 房间状态怎么收敛

`RoomState` 是跨节点同步的最小一致单元，带一个单调递增的 `revision`。

```rust
pub struct RoomState {
    pub id: String,
    pub owner: Option<String>,   // None = 归属未确定
    pub media_addr: String,
    pub node_role: NodeRole,
    pub streams: u64,            // 上行媒体流条数
    pub listeners: u64,          // 旁听参会者数
    pub revision: u64,           // 单调递增消息序号
}
```

规则很简单：收到同一房间的更新时，只接受 `revision >=` 当前值的，
并且**始终覆盖**本地视图。因为 revision 是单调递增的消息序号，覆盖即收敛 ——
不需要 Raft、不需要选主、不需要额外仲裁。

`owner == None` 是两段式交接的安全锚点：刚创建还没等到调度回包的房间不参与调度，
也不会被当成孤儿迁移。

### 1.4 新会议调度到负载最低的节点

负载分数是两个归一化维度相加：

```
load = rooms_ratio + listeners_ratio     # 各归一到 0..=1000，总和 0..=2000

rooms_ratio     = rooms_on(node) * 1000 / max_rooms
listeners_ratio = listeners_on(node) * 1000 / listener_weight
listener_weight = listener_fanout * listener_capacity
```

这样「很多小会议」和「很少但超大的旁听会议」能放在同一把尺子上比较 ——
如果只数会议数，一个 10000 人旁听的大房间和一个空房间会被当成等重。

**平局按 `node_id` 字典序打破。** 这不是随意选择的：三个节点各自计算「谁最闲」
时必须得出**完全一致**的结论，否则会出现两个节点同时认为自己该接同一个会议。
字典序保证确定性，不需要任何分布式协议来对齐。

调度请求走 NATS **队列订阅**：三个节点都订阅 `qm.<cid>.route.new` 但共享一个队列，
NATS 只投递给队列里的一个节点，由它回包。请求方用 `request_timeout_secs` 兜底。

### 1.5 健康检查与故障迁移

```
判死窗口 = heartbeat_secs × unhealthy_misses = 5s × 2 = 10s
```

默认值直接对齐验收标准 2：节点连续错过 2 次心跳（10 秒）即判死，
随后开始迁移它名下的房间。

心跳发送后**立即 flush**，确认消息已到 server。这一步看起来多余，
但它是故障检测正确性的前提：只有「最后一次心跳确实送达」，
节点掉线才会被正确判死，而不是被当成「网络抖动」。

`request_timeout_secs` 必须小于 `heartbeat_secs`（配置校验期强制）：
否则一次请求可能挂到下一次心跳之后，把「请求超时」和「节点故障」混成一件事。

**迁移是两段式的，源节点不直接改归属。**

```
判死节点 X
   │
   ▼
每个存活节点各自计算「X 名下的房间该迁到谁」（本地判断，不依赖协调者）
   │
   ▼
向候选目标 Y 发迁移请求：qm.<cid>.migrate.req.<Y>
   │
   ├── Y 有余量 + 健康 → 改归属为 Y，发布新的 RoomState（全集群广播）+ 回包 ok
   │
   └── Y 拒绝（无余量/未健康/发布失败）→ 回包 reject，发起方试下一个候选
```

两个关键取舍：

1. **迁移请求定向投递，不走队列。** 发起方已经选定了目标节点，
   队列会让任意节点抢走这条消息然后被直接忽略。
2. **故障节点无法发起迁移，所以每个存活节点各自本地判断。**
   节点 X 崩溃后它自己不可能发任何东西，必须靠其他节点发现「X 名下的房间
   还没人接」并接手。判死窗口 + 迁移窗口合计约 10s，落在验收标准 2 内。

### 1.6 旁听容量

加/减旁听者**只更新计数并发布**，不搬运媒体载荷。旁听者的实际媒体转发由
`node_role = listener` 的节点承担：

```
单节点旁听槽位 = listener_fanout × listener_capacity
             = 200 × 20000 = 4,000,000
```

默认配置下单节点的旁听容量口径是 400 万槽位，远超验收标准 3 的 10000 人。
`listener_fanout` 表示「一条上行流折算多少只收流观众」：
SFU 对下行观众只重传不重新编码，所以一条上行流服务上千个只收流观众是可行的。
旁听节点不参与调度候选（`is_schedulable()` 返回 false），
它们只承接已经被分配出去的房间的旁听转发。

---

## 2. 部署

### 2.1 3 节点集群（验收标准 1）

`docker-compose.yml` 已经内置 3 个媒体节点 + 1 个 NATS + 1 个信令：

```bash
docker-compose up -d --build
docker-compose ps
```

节点身份靠环境变量区分，不需要改镜像。媒体节点的 `command` 显式传 `--cluster`
进入集群模式（Dockerfile 的 `CMD` 默认不带这个参数，只是跑验证报告后退出）：

```yaml
qm-media:
  command: [--bind, 0.0.0.0, --cluster]   # 没有 --cluster 就不会进集群
```

| 服务 | node_id | 媒体地址 | 容器端口 |
| --- | --- | --- | --- |
| `qm-media` | `node-1` | `192.168.0.10:8080` | `8080` / `8081` |
| `qm-media-2` | `node-2` | `192.168.0.11:8082` | `8082` / `18081` |
| `qm-media-3` | `node-3` | `192.168.0.12:8083` | `8083` / `18082` |
| `qm-nats` | — | — | `4222` / `8222` |
| `qm-signaling` | — | — | `8081`（`--signal` 模式） |

新会议由调度器按负载分配，三个节点会各自分到一部分 —— 不需要手工指定会议落哪。

**媒体节点上没有 HTTP 存活探针**：集群模式只监听媒体端口，不监听 8081，
没有可用的 HTTP 端点。容器存活由 `restart: unless-stopped` 保证，
集群层面（NATS 连通性）由节点自己的心跳循环保证。`qm-nats` 有健康检查
（`8222/-/health`）。

### 2.2 配置

配置三层覆盖（后者覆盖前者）：

```
config/default.toml  →  config/local.json  →  环境变量 QM_SECTION_FIELD
```

字段名**只按第一个 `_` 分层**：`QM_CLUSTER_NODE_ID` → `cluster.node_id`。
多段字段名的索引写法（`QM_CLUSTER_X_0`）会被直接拒绝，不会静默退化。

`cluster` 段全部字段：

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `server` | `127.0.0.1` | NATS 地址；容器内用容器名 `qm-nats` |
| `port` | `4222` | NATS 客户端端口 |
| `cluster_id` | `quickmeet` | 集群标识，多集群共用一个 NATS 时隔离用 |
| `node_id` | `node-1` | 节点唯一标识 |
| `advertised_addr` | `192.168.0.10:8080` | 对外媒体地址 |
| `node_role` | `full` | `full` 全功能 / `listener` 旁听分发 |
| `heartbeat_secs` | `5` | 心跳间隔 |
| `unhealthy_misses` | `2` | 错过几次判死（窗口 = 5×2 = 10s） |
| `failover_target_secs` | `10` | 故障后迁移完成时限 |
| `join_target_secs` | `30` | 新节点接入时限 |
| `request_timeout_secs` | `3` | 请求超时，必须小于 `heartbeat_secs` |
| `max_rooms_per_node` | `64` | 单节点会议数硬上限 |
| `listener_capacity` | `20000` | 单节点旁听上限 |
| `listener_fanout` | `200` | 单流旁听折算系数 |

### 2.3 合规校验（启动期强制）

以下情况会**拒绝启动**，不会带着错误配置默默跑：

- `cluster.server` 或 `advertised_addr` 指向公网地址 → 拒绝（数据不得出域）；
- 地址不在 `network.cidrs` 声明的内网网段内 → 拒绝；
- `advertised_addr` 不是 `host:port` 形式的 IPv4 → 拒绝；
- `request_timeout_secs >= heartbeat_secs` → 拒绝；
- 任何关键字段为 0 或空 → 拒绝。

**唯一例外是回环地址 `127.0.0.1`。** 这是 NATS 与节点同机部署（本机起 NATS、
或 sidecar 模式）的典型写法，回环流量不离开本机，仍满足数据不出域。
容器化部署时 `cluster.server` 应该写容器名 `qm-nats`，而不是 `127.0.0.1` ——
后者只会连到容器自己，那里没有 NATS。

`cluster.server` 只接受 IPv4 字面量，不接受域名：避免 DNS 解析到公网地址绕过
校验，也避免解析延迟被误判成连接失败。

### 2.4 docker-compose 1.29.2 兼容性

`docker-compose.yml` 已按 1.29.2 约束编写：

- 无 `deploy:` 段（1.29.2 直接拒绝）；
- 无 YAML anchor / alias（v3.x 规范不支持）；
- 无 `extends:` / `develop:` / `secrets:` / `config:`；
- 重启策略用服务级 `restart:`，不写在 `deploy.restart_policy`；
- `build` 用短语法，context 不越过仓库根；
- 节点身份用每服务独立的 `environment:`，靠复制服务块而非 anchor 复用。

NATS 镜像固定到 patch 版本 `nats:2.10.21-alpine`（不是 `:2.10` 或 `:latest`），
保证私有化环境下镜像可复现、可在断网环境预拉取。

### 2.5 NATS 客户端版本选型（验收标准 1 的实际约束）

`async-nats` 固定 **`0.37.0`**，这个版本号不是随手选的：

| 版本 | 能否在 Rust 1.75 上编译 | 原因 |
| --- | --- | --- |
| `0.37.0` | ✅ 可以 | 最后一个不依赖 `tokio-websockets` 的版本 |
| `0.38.0` 起 | ❌ 不行 | 强制依赖 `tokio-websockets` 0.10（`rust-version = 1.79`）；即使 `default-features = false` 也绕不过，`ring` feature 同时启用 `tokio-websockets/ring` |
| `0.46.0` 起 | ❌ 不行 | 自身声明 `rust-version = 1.79` |
| `0.47.0` / `0.50.0` | ❌ 不行 | 自身声明 `rust-version = 1.88` |

也就是说：**MSRV 1.75 与 core NATS 客户端的交集只剩 `<=0.37`**。
本 crate 只用到 core NATS 的 pub/sub 与 request/reply（房间状态同步、
心跳、调度、迁移），不需要 websocket 传输层，所以 0.37 足够。
升级前必须先确认 MSRV 也一起提升 —— 否则 `cargo check` 在 1.75 上直接失败。

0.37 与 0.38 的唯一接口差异是 `Client::drain()`（0.38 才有），
`NatsBus::shutdown` 用 `flush()` 替代，语义在进程退出前等价。

---

## 3. 扩容操作说明

### 3.1 加一个媒体节点（扩容）

`docker-compose.yml` 里已经有可复制的模板。四步：

```bash
# 1. 复制 qm-media-3 整段，改成 qm-media-4，改这三处：
#      container_name: qm-media-4
#      ports: ["8084:8080", "18083:8081"]
#      environment:
#        - QM_CLUSTER_NODE_ID=node-4
#        - QM_CLUSTER_ADVERTISED_ADDR=192.168.0.13:8084
#      volumes: 换成 meeting-data-4，并在 volumes: 段声明 meeting-data-4

# 2. 起新节点（只起这一个，不动现有节点）
docker-compose up -d qm-media-4

# 3. 确认它加入了集群：日志里应出现「节点已加入集群」
docker-compose logs -f qm-media-4

# 4. 观察它是否开始承接新会议（负载应开始向它倾斜）
docker-compose logs -f qm-media-4 | grep -E "调度|承接"
```

复制服务块时**三处都要改**，漏一处就会出现两个同名节点互相踢心跳：

1. `container_name` 与 `service` 名（如 `qm-media-4`）；
2. `environment` 里的 `QM_CLUSTER_NODE_ID` 与 `QM_CLUSTER_ADVERTISED_ADDR`；
3. `ports` 映射 + 对应的 `meeting-data-*` volume（并在文件底部 `volumes:` 段声明）。
另外记得加上和 node-1/2/3 一样的 `command: [--bind, 0.0.0.0, --cluster]`。

**不需要重启任何现有节点。** 成员关系由心跳维护，新节点上线后其他节点
在一个心跳窗口内就会看到它。验收标准 4 要求 30 秒内接入并开始承接新会议 ——
默认配置下新节点连接 NATS、订阅 subject、拉到全量快照后立即可被调度，
实际耗时通常远小于一个心跳周期。

### 3.2 加一个旁听节点（放大旁听人数）

旁听节点不参与调度，只转发已分配房间的旁听流。用一条命令临时起一个：

```bash
docker-compose run --rm -d --name qm-listener-1 \
  -p 9080:8080 \
  -e QM_CLUSTER_NODE_ID=listener-1 \
  -e QM_CLUSTER_ADVERTISED_ADDR=192.168.0.20:9080 \
  -e QM_CLUSTER_NODE_ROLE=listener \
  qm-media-2
```

或长期运行就把它写进 `docker-compose.yml`，`environment` 里加
`QM_CLUSTER_NODE_ROLE=listener`。旁听节点不需要独立的 `meeting-data-*` volume
（它不存会议数据），但建议挂 `meeting-logs` 便于排查。

单节点旁听容量 = `listener_fanout × listener_capacity` = 200 × 20000 = 400 万槽位。
要承接更大旁听人数就调高这两个字段；媒体转发能力（CPU / 带宽）仍是实际瓶颈，
槽位是调度上限而非吞吐保证。

### 3.3 缩容（减节点）

```bash
# 优雅下线：先停，让该节点名下的会议被迁移走
docker-compose stop qm-media-3

# 确认其他节点已经接管它的房间
docker-compose logs qm-media-2 | grep -E "迁移|承接"

# 确认无会议归属在 node-3 之后，再清理
docker-compose rm -s qm-media-3
docker volume rm quickmeet_meeting-data-3   # 需要时
```

停掉节点后它会停止心跳，其他节点在一个判死窗口（默认 10s）内判死它并迁移
其房间。会议数据本身不会丢 —— 各节点的媒体数据在自己的 named volume 里。

### 3.4 水平扩容是近线性的

承载能力随节点数近似线性提升，前提是：

- NATS 只有一个，但它只转发**控制面消息**（心跳、房间状态、调度命令），
  不承载任何媒体流，所以它不是瓶颈；
- 每个节点是独立容器，媒体转发资源（CPU / 带宽）按节点线性增加；
- 调度按负载分数分配，新节点上线后自动分摊新会议。

非线性的部分：旁听节点承接的是已分配房间的转发，它的容量受媒体节点的上行流
数量约束（`listener_fanout` 折算）。媒体节点是 1，旁听节点再多也不能凭空
增加上行流。

### 3.5 故障演练（验证验收标准 2）

```bash
# 造一个故障节点
docker-compose kill -s KILL qm-media-3

# 观察迁移：其余节点应在 ~10s 内判死 node-3 并迁移其房间
docker-compose logs -f qm-media qm-media-2 | grep -E "节点故障|触发故障迁移|已承接迁移"
```

时间线（默认配置）：

```
t=0s     node-3 停止心跳
t=0-5s   第 1 次心跳错过（窗口未耗尽）
t=10s    第 2 次心跳错过 → 判死，开始迁移
t≤10s    迁移完成，房间归属到负载最低的健康节点
```

单节点故障不影响其他节点的会议：房间归属是 per-room 的，
判死 node-3 只动 node-3 名下的房间，node-1 / node-2 的会议完全不受影响。

---

## 4. 配置速查

| 目标 | 做法 |
| --- | --- |
| 3 节点集群 | `docker-compose up -d --build`（已内置） |
| 加媒体节点 | 复制一个 `qm-media-*` 服务，改 `node_id` / `advertised_addr` / 端口 |
| 加旁听节点 | 加 `QM_CLUSTER_NODE_ROLE=listener` |
| 缩短故障恢复时间 | 调小 `QM_CLUSTER_HEARTBEAT_SECS`（如 3）并同步调小 `REQUEST_TIMEOUT_SECS` |
| 提高单节点会议上限 | `QM_CLUSTER_MAX_ROOMS_PER_NODE=128` |
| 提高旁听容量 | `QM_CLUSTER_LISTENER_CAPACITY` × `QM_CLUSTER_LISTENER_FANOUT` |
| 切换 NATS 地址 | `QM_CLUSTER_SERVER=<内网 IPv4>`（容器内用容器名） |
| 隔离测试集群 | `QM_CLUSTER_CLUSTER_ID=qm-test` |

---

## 5. 本地验证

调度、健康判定、迁移目标选择都是纯函数，不需要 NATS 就能单测：

```bash
cargo test -p qm-cluster          # 18 个单测：subject 规划、调度、判死、迁移目标、收敛、进程入口
cargo test -p qm-common           # 28 个单测：含 cluster 配置校验与合规拒绝
cargo test --workspace            # 全量 166 个测试
```

已覆盖的验收逻辑：

| 验收标准 | 测试 |
| --- | --- |
| 1. 3 节点部署，会议分配到不同节点 | `assign_returns_none_when_no_candidates`、`cluster_defaults_satisfy_acceptance_windows` |
| 2. 节点宕机 10s 内迁移 | `cluster_defaults_satisfy_acceptance_windows`（判死窗口 = 5×2 = 10s） |
| 3. ≥10000 人旁听容量 | `cluster_defaults_satisfy_acceptance_windows`（200×20000 = 400 万槽位） |
| 4. 30s 内接入 | `cluster_defaults_satisfy_acceptance_windows`（join_target_secs = 30） |
| 全局约束 1/3/5 | `cluster_validate_rejects_public_addresses`、`cluster_validate_rejects_request_timeout_not_under_heartbeat` |

---

## 6. 运维要点

### 6.1 NATS 不可达时节点退化为单机模式

连接失败**不会让进程退出**。节点退化为单机模式继续开会，
下一跳心跳重试连接，NATS 恢复后自动重新加入集群。

这是「节点动态增减、无需重启整体服务」的前提：一个节点启动慢一点、
NATS 短暂重启、网络抖动，都不会造成会议中断。

副作用：单机模式下的新会议不会被集群调度（只在本节点），
也不会被其他节点感知。恢复连接后靠一次全量快照对齐状态。

### 6.2 NATS 本身宕机

NATS 是单点（无 JetStream 持久化）。宕机期间：

- 所有节点退化为单机模式，现有会议继续；
- 不产生新的判死判定，也不会触发迁移；
- NATS 恢复后各节点重新订阅并交换快照，房间视图重新收敛。

会议状态不会持久化到 NATS，所以 NATS 重启**不会丢失**正在进行的会议 ——
它们在各节点的内存里，重建连接后通过快照互相同步。

### 6.3 常见问题

| 现象 | 原因 / 处理 |
| --- | --- |
| 启动报「地址不在允许的内网网段」 | `cluster.server` 是公网地址或域名。改内网 IPv4，或加网段到 `QM_NETWORK_CIDRS` |
| 启动报「request_timeout_secs 必须小于 heartbeat_secs」 | 调小超时或调大心跳间隔 |
| 启动报「cluster.advertised_addr 需要 host:port 形式」 | 补端口，如 `192.168.0.11:8082` |
| 日志显示「节点已加入集群」但集群节点数不变 | 检查 `cluster_id` 是否一致；多集群共用 NATS 时靠它隔离 |
| 容器内连不上 NATS | 用容器名 `qm-nats`，不是 `127.0.0.1`（回环连的是容器自己） |
| 迁移一直失败 | 目标节点可能无余量（`max_rooms_per_node` 满）或被判为不健康；调大上限或检查心跳 |
| 新节点起来了但不承接会议 | 确认 `node_role` 是 `full`；`listener` 节点不参与调度候选 |

### 6.4 与上游/下游 Issue 的关系

- **QM-005（YEJ-94，房间模型与权限）**：`RoomState` 的 `id` / `owner` 语义与房间模型对齐。
- **QM-006（本 Issue）**：集群层只提供房间归属与调度决策，
  不实现媒体转发本身的旁听扩展 —— 旁听的实际转发能力由后续 Issue 在
  `qm-sfu` / `qm-media` 里承接。
- 本 crate 不持久化任何媒体载荷，只同步房间状态（全局约束 5）。

---

## 7. 已知的边界与后续项

1. **NATS 是单点。** 未做 NATS server 集群（`-c` 组网）。
   私有化 3 节点规模下可接受；生产环境建议在 NATS 层做高可用。
2. **`join_target_secs` 目前是配置项而非硬超时。** 代码保证新节点上线后立即
   可被调度（连接 NATS + 拉快照后），30s 是验收时限口径，不是强制拦截。
3. **故障迁移是请求/回复，非推送。** 故障节点无法主动发起迁移，
   靠每个存活节点各自本地判断并驱动，因此迁移发起方可能不止一个。
   `RoomState.revision` 单调递增保证了重复迁移请求不会产生双归属 ——
   后到的同房间状态会收敛，但会产生一次多余的迁移尝试。
4. **媒体容量折算基于 `listener_fanout` 的经验系数。** 实际瓶颈是媒体节点的
   CPU / 带宽，建议在真实 10000 人旁听场景中校准该系数。
5. **本开发机没有 `nats-server` 也没有 Docker，无法在本地跑通 3 节点集成测试，
   也无法验证 docker-compose 1.29.2 的运行时行为。** 调度/健康/迁移逻辑
   已用纯函数单测覆盖（`cargo test --workspace` 166 个测试全绿），
   但以下三项**必须**在部署环境实测，本 Issue 无法自证：
   * 验收标准 1 的多节点分配（§3.1）；
   * 验收标准 2 的 10s 内迁移（§3.5 故障演练）；
   * 验收标准 4 的 30s 接入（§3.1 第 3 步）。
6. **媒体节点没有 HTTP 存活探针**（见 §2.1）。集群模式下容器内没有可探测的
   HTTP 端口，只有 `restart: unless-stopped`。如果需要容器级探针，
   建议后续给媒体端口加一个 TCP 探针（1.29.2 支持
   `test: ["CMD-SHELL", "wget -qO- http://127.0.0.1:8080/ >/dev/null"]`）。
