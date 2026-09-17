# QuickMeet Stage 1 — 运行说明（RUNBOOK）

对应 Issue **YEJ-90**（父 Epic YEJ-89）。本文档是可复现的操作步骤；命令均为
本机（Windows / MinGW）实测通过的形式。

## 1. 环境

| 项 | 要求 |
| --- | --- |
| Rust | 稳定版 **1.75+**（已按 MSRV 1.75 校验） |
| C 工具链 | 需要 `gcc`（用于 `-sys` / proc-macro 构建脚本） |
| 私网 | 默认只允许 `192.168.0.0/24` |
| Docker | `docker-compose` **1.29.2**（compose 文件按 1.29.2 语法写，未使用 `deploy:` 等 2.x 专属特性） |

依赖全部来自 crates.io，**没有私有依赖**。

## 2. 三种构建/运行方式

```bash
# A. 编解码收发兼容验证（最快，秒级完成，产出控制台报告）
cargo build --release
./target/release/qm-demo

# B. 完整回归测试（209 个测试，workspace 全部 crate）
cargo test --workspace

# C. 起信令服务（长跑，Ctrl+C 退出；含 /healthz 探活）
QM_NETWORK_BIND_HOST=127.0.0.1 ./target/release/qm-demo --signal
```

`--bind 127.0.0.1` 等价于 B 里的环境变量，但优先级更高（覆盖配置文件）。

## 3. 验证结果如何复现

```bash
# 3 个 codec 逐项验证，每 codec 20 帧
./target/release/qm-demo --frames 20

# 额外落盘 JSON 报告到 ./data/meetings/（本地存储，不出域）
./target/release/qm-demo --frames 20 --json-report

# 信令服务探活
QM_NETWORK_BIND_HOST=127.0.0.1 QM_MEDIA_SIGNALING_PORT=18191 \
  ./target/release/qm-demo --signal --bind 127.0.0.1
curl http://127.0.0.1:18191/healthz
```

## 4. 配置覆盖顺序

后者覆盖前者：

1. `config/default.toml`（仓库内默认值）
2. `config/local.json`（可选，存在才加载；本机/现场覆盖）
3. 环境变量 `QM_SECTION_FIELD`

环境变量**只按第一个 `_` 分层**，后面的下划线属于字段名：

| 环境变量 | 生效配置项 |
| --- | --- |
| `QM_MEDIA_PORT` | `media.port` |
| `QM_MEDIA_SIGNALING_PORT` | `media.signaling_port` |
| `QM_NETWORK_BIND_HOST` | `network.bind_host` |
| `QM_NETWORK_CIDRS` | `network.cidrs`（必须是 JSON 数组） |
| `QM_LOGGING_FILE_DIR` | `logging.file_dir` |
| `QM_STORAGE_DATA_DIR` | `storage.data_dir` |
| `QM_AI_BASE_URL` | `ai.base_url` |
| `QM_AI_TIMEOUT_MS` | `ai.timeout_ms` |

两条容易踩的规则：

- **写错字段名会直接报错**，不会静默忽略。例如
  `QM_NETWORK_CIDRS_0=1.2.3.4/32` 会被拒绝：
  `环境变量 QM_NETWORK_CIDRS_0 指向未知的配置项 network.cidrs_0`。
  这是有意为之 —— 「配置看起来生效其实没生效」比直接失败难查得多。
- **Vec 字段只能用 JSON 数组**：`QM_NETWORK_CIDRS=["192.168.0.0/24","10.0.0.0/8"]`。

## 5. 已知未在本机验证的事项（如实记录，未静默跳过）

### 5.1 webrtc-rs 0.17.1 双端 PeerConnection（验收标准 2）

代码已实现于 `crates/qm-media/src/peer_connection.rs`（双端拓扑、SDP 交换、
ICE candidate、Opus 轨道收发、data channel），但**本机构建不了**：

```
webrtc 0.17.1 -> webrtc-sys 0.11 -> ring 0.17 需要 MSVC cl.exe 编译 C 代码
本机只有 MinGW（gcc / clang / nasm / cmake），没有 cl.exe
```

在有 MSVC 的机器上执行即可复现：

```bash
cargo test -p qm-media --features webrtc
```

`webrtc` 是 optional feature，**默认不编译**，因此默认构建零外部 C 工具链依赖。
另外 `crates/qm-media` 曾依赖 `webrtc-media` 与 `rtp`，但源码里只有
`peer_connection.rs` 用到 `webrtc::*`；移除这两个死依赖后，
`webrtc-util 0.17.2`（MSRV 1.86，用到 `usize::is_multiple_of`）整棵子树都不再进入依赖图。

### 5.2 Docker 镜像与 compose

`Dockerfile` 与 `docker-compose.yml` 已按 1.29.2 语法约束编写（无 `deploy:`、
无 YAML anchor、无 `extends`/`develop`/`secrets`/`config`/`include`、
service 级 `restart:`、短语法 `build`）。但本机没有 `docker` / `docker-compose`，
**未实际执行过**，需要现场验证：

```bash
docker-compose up -d --build
docker-compose logs -f qm-media
docker-compose ps
docker-compose down
```

## 6. 排错速查

| 现象 | 原因 / 处理 |
| --- | --- |
| `payload type N 超过 7 位上限` | PT 只占 byte1 的 7 位（RFC 3550）。marker 在 byte1 最高位，不在 bit5 |
| `配置解析失败: missing field ...` | 配置段缺字段。所有 `*Config` 结构体都带 `#[serde(default)]`，此报错说明是新加的段没加 |
| 环境变量设了但没生效 | 检查字段名是否在 allowlist（见上文 §4 表格）；拼错会直接报错而非静默 |
| 日志级别组合不生效 | `info,webrtc=debug,qm_media=trace` —— 逗号后**不能有空格** |
| 绑不上 `192.168.0.10` | 本机无该网卡；用 `--bind 127.0.0.1` 或 `QM_NETWORK_BIND_HOST=127.0.0.1` |
