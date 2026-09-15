# QuickMeet

基于 webrtc-rs 的 AI 赋能私有化极速视频会议系统。**Stage 1** 交付：Cargo workspace
+ tokio 骨架 + 可复用的 VP8 / H.264 / Opus 编解码封装模块与收发兼容验证。

对应 Issue **YEJ-90**（父 Epic **YEJ-89**）。

## 快速开始

```bash
cargo build --release
./target/release/qm-demo                      # 编解码收发兼容验证报告
cargo test --workspace                         # 209 个回归测试
QM_NETWORK_BIND_HOST=127.0.0.1 \
  ./target/release/qm-demo --signal --bind 127.0.0.1   # 起信令服务
./target/release/qm-demo --cluster                     # 集群模式（QM-006，需 NATS）
```

集群部署与扩容见 `docs/CLUSTER_DESIGN.md`；`docker-compose up -d --build`
内置 1 个 NATS + 3 个媒体节点 + 1 个信令服务。

MSRV **1.75**，依赖全部来自 crates.io，无私有依赖。
详细步骤见 `docs/RUNBOOK.md`。

## 目录结构

| 路径 | 说明 |
| --- | --- |
| `crates/qm-common` | 错误模型（统一 `Error`）、配置加载、日志初始化、本地持久化、CIDR 校验 |
| `crates/qm-media` | 可复用编解码封装：RTP（RFC 3550）、帧模型、codec registry、bitstream 打包、收发验证 |
| `crates/qm-signaling` | tokio + hyper 信令服务，`GET /healthz` 探活、房间 peer 列表 |
| `crates/qm-sfu` | SFU：媒体路由、选择性转发、NACK/FEC 恢复、硬件加速、容量基准（200+ 流） |
| `crates/qm-cluster` | 分布式集群：NATS 房间状态同步、最低负载调度、健康检查与故障迁移（QM-006） |
| `demos/qm-demo` | 可本地运行的 demo：跑验证报告 + 起信令服务 + 集群模式 |
| `config/` | `default.toml`、`local.json.example`、`container.env` |
| `docs/RUNBOOK.md` | 构建/运行/验证/排错操作手册 |
| `docs/CLUSTER_DESIGN.md` | 集群架构、部署与扩容操作说明（QM-006） |
| `Dockerfile` `docker-compose.yml` | 容器化（按 docker-compose 1.29.2 语法编写，内置 3 节点集群） |

## 编解码模块

编解码走纯 Rust 确定性实现（无损容器格式 + CRC32 完整性校验），
**默认构建不引入任何第三方编解码 crate**，因此 `cargo build --release`
零外部 C 工具链依赖：

| codec | payload type | clock | 载荷头长 |
| --- | --- | --- | --- |
| VP8 | 96 | 90000 Hz | 24 B |
| H.264 | 100 | 90000 Hz | 30 B |
| Opus | 111 | 48000 Hz | 22 B |

可选原生特性（默认全关，需要时自行开启）：

```bash
cargo test -p qm-media --features native-opus      # opus-sys
cargo test -p qm-media --features native-h264      # openh264
cargo test -p qm-media --features native-vp8       # vpx
cargo test -p qm-media --features webrtc           # webrtc-rs 0.17.1（见下方阻塞说明）
```

载荷头长度是**单一契约来源**（`CodecId::payload_header_len()`），
编码器写多少字节、解码器与 CRC 校验从哪里开始，都从这里取值 —— 避免两端各写一份常量
再悄悄对不上。

## 全局约束落实（继承 Epic YEJ-89）

| 约束 | 落实方式 |
| --- | --- |
| 兼容 Rust 1.75+ 稳定版 | `rust-version = "1.75"`；`.cargo/config.toml` 开 `incompatible-rust-versions = "fallback"`；已用 1.75.0 实跑 `cargo check --workspace` |
| 无私有不可访问依赖 | 全部 crates.io；workspace 锁版本号固定 |
| 兼容 docker-compose 1.29.2 | compose 文件不用 `deploy:`、YAML anchor、`extends`/`develop`/`secrets`/`config`/`include`；service 级 `restart:`；短语法 `build` |
| 媒体服务默认监听 8080 | `config/default.toml` 的 `media.port = 8080` |
| 适配内网 192.168.0.0/24 | 默认 `network.cidrs = ["192.168.0.0/24"]`，`bind_host = "192.168.0.10"`；CIDR 启动期校验 |
| AI 只对接本地硅基流动 | `ai.enabled = false`，`ai.base_url` 默认 `http://127.0.0.1:3000/v1`；第一阶段仅保留接入点 |
| 音视频/会议数据本地存储 | `storage.data_dir = "./data/meetings"`，验证报告落盘在此；无任何公网 API 调用 |

## 已知阻塞项（如实记录，未静默跳过）

### webrtc-rs 0.17.1 双端 PeerConnection 未在本机构建（验收标准 2）

代码已实现于 `crates/qm-media/src/peer_connection.rs`：双端拓扑、SDP 交换、
ICE candidate 传输、Opus 轨道收发、data channel；`RTCConfiguration.ice_servers`
留空只用 host candidate（内网不需要 STUN/TURN，也不会把本机地址上传公网）。

但本机无法构建：

```
webrtc 0.17.1 -> webrtc-sys 0.11 -> ring 0.17 需要 MSVC cl.exe
本机只有 MinGW（gcc / clang / nasm / cmake）
```

`webrtc` 是 optional feature、默认不编译，因此**默认构建不受影响**。
在有 MSVC 的机器上执行 `cargo test -p qm-media --features webrtc` 即可复现。

顺带修掉一个 MSRV 隐患：`qm-media` 之前声明了 `webrtc-media` 与 `rtp` 两个依赖，
但源码里只有 `peer_connection.rs` 用到 `webrtc::*`；移除后 `webrtc-util 0.17.2`
（MSRV 1.86，用 `usize::is_multiple_of`）整棵子树退出依赖图，1.75 才能通过。

### Dockerfile / docker-compose.yml 未实际执行

本机没有 `docker` / `docker-compose`，容器化部分是按 1.29.2 语法约束编写的，
**未经运行验证**，需现场执行 `docker-compose up -d --build` 确认。

## 贡献

见 `docs/RUNBOOK.md` 的构建与测试章节。
