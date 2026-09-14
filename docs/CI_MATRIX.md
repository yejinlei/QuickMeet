# QuickMeet CI 验收矩阵（QM-018）

对应 Issue **QEJ-109 / QM-018**。目的：每条 QM 验收标准都能指到**具体的 CI job**
或**具体的本地命令**，不再出现「这条怎么验没人说得清」。

## 0. CI job 一览

| Job 名（`.github/workflows/ci.yml`） | 运行环境 | 做什么 | 超时 |
| --- | --- | --- | --- |
| `rust-msrv` | ubuntu-22.04 + Rust **1.75** | MSRV 检查、`fmt --check`、`clippy`（本 PR 触碰的 crate 硬挡 `-D warnings`；workspace 全量只作提示）、`cargo test --workspace --locked`、`cargo build --release`、`qm-demo` 默认命令 + 端口假设 | 15 min |
| `compose-legacy` | ubuntu-22.04 + docker + **docker-compose 1.29.2** | Dockerfile 构建、compose 语法与 1.29.2 兼容校验、`up -d --build`、五容器健康检查、`/healthz` 探活、逐个 restart 自愈验证 | 15 min |
| `windows-webrtc` | windows-latest + MSVC `cl.exe` | `cargo test -p qm-media --features webrtc --locked`（本机只有 MinGW，跑不到这一条） | 15 min |
| `pr-title` | ubuntu-22.04 | PR 标题必须匹配 `^(QM-\d{3}\|YEJ-\d+): .+`，固化标题约定并保证 Issue 自动关联 | 5 min |

约束达成情况（Issue 强制约束）：

- **无私有凭据**：全部工件来自 crates.io、Docker Hub、GitHub 官方 release；
  workflow 里没有任何 token 字面量，`permissions: contents: read`。
- **无第三方 SaaS**：不接监控/构建服务，只有 GitHub Actions 自身。
- **compose 1.29.2**：CI 里装的是**真实 1.29.2 二进制**（不是 `docker compose`
  v2 语法校验），并且 `verify.sh` 步骤 7 额外做 2.x 专属键黑名单扫描
  （`deploy` / `extends` / `develop` / `secrets` / `config` / `include` / `target`）。
- **≤15 分钟**：每个 job 单独 `timeout-minutes`，最长的 `compose-legacy` 是 15。

## 1. 本地一键验证

```bash
bash scripts/verify.sh                # 全部 9 步（需要 docker + docker-compose）
bash scripts/verify.sh --no-docker    # 跳过容器步骤（本机没有 Docker 时用）
STEPS="1 3 4 5" bash scripts/verify.sh --no-docker   # 只跑指定步骤
```

步骤编号在本地与 CI 里是**同一套**（脚本头部的注释与下表一一对应）：

| 步骤 | 内容 | CI job |
| --- | --- | --- |
| 1 | MSRV 1.75：`Cargo.toml` 声明 + 本机 `rustc` 版本 ≥ 1.75 | `rust-msrv` |
| 2 | `cargo fmt --check` + `cargo clippy --all-targets -p qm-common -p qm-cluster -- -D warnings`（workspace 全量 clippy 仅提示，见脚本注释） | `rust-msrv` |
| 3 | `cargo test --workspace --locked --no-fail-fast` | `rust-msrv` |
| 4 | `cargo build --release --locked --workspace` | `rust-msrv` |
| 5 | `qm-demo --bind 127.0.0.1 --frames 2` + `config/default.toml` 里 8080/8081 断言 | `rust-msrv` |
| 6 | `docker build -t quickmeet-verify:local .` | `compose-legacy` |
| 7 | compose 1.29.2 兼容黑名单扫描 + `docker-compose config` | `compose-legacy` |
| 8 | `docker-compose up -d --build` + 五容器全部 `healthy` + 四个 `/healthz` 返回 200 | `compose-legacy` |
| 9 | 逐个 `docker restart`，验证 `restart: unless-stopped` 自愈、无循环重启 | `compose-legacy` |

> 步骤 8 里重启次数 >6 次就判定「循环重启」并失败；这是 QM-015 验收标准 2 的机器化判据。

## 2. 各 QM 验收标准的覆盖

「覆盖」分三档，**不要含糊**：

- **自动**：CI 里有一个 job/步骤会直接判 PASS/FAIL，红了就挡住合并。
- **半自动**：CI 里跑了相关检查，但还需要人看一眼产物（日志/报告）才能下结论。
- **未覆盖**：CI 里**没有**检查，只能现场手工验。这类必须写清命令与前置条件。

### QM-001 项目骨架与编解码

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | `cargo build` 无错误无警告 | 自动 | `rust-msrv` 步骤 2b（本次改动范围 clippy `-D warnings`）+ 步骤 4（`--release`）；workspace 全量 clippy 仍有历史 warning，见 `rust-msrv` 步骤 2c 的提示输出 |
| 2 | 两个浏览器 tab 互通音视频 | 未覆盖 | 需要浏览器 + 真实摄像头；CI 无浏览器环境。本地：`qm-demo --signal` 后手动开两个 tab |
| 3 | VP8 / H.264 切换、Opus 无中断 | 半自动 | `qm-media` 的 codec 单测（步骤 3）断言编解码对称性、PT/时钟字段、确定性；「画面正常/清晰」无法机器判定 |
| 4 | Docker 镜像可构建、端口映射正确 | 自动 | `compose-legacy` 步骤 6（构建）+ 步骤 7（`config` 里端口映射）+ 步骤 8（8081/8091/8092/8093 探活返回 200） |

### QM-002 SFU 选择性转发

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | 3 tab 同会互见 | 未覆盖 | 需要浏览器端；当前仓库还没有前端（QM-012 交付） |
| 2 | 摄像头/屏幕共享切换 | 未覆盖 | 同上，依赖前端与真实媒体源 |
| 3 | 内网 NAT 连接成功率 ≥99% | 未覆盖 | 需要跨机压测；当前 CI 只跑单机容器 |
| 4 | CPU 相比 MCU 降低 ≥30% | 未覆盖 | 需要基线对照（MCU 模式）与压测，见 `bench/` 脚本，手工执行 |

### QM-003 抗丢包 / 硬件加速 / 容量

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | 30% 丢包下无花屏、音频无中断 | 半自动 | `qm-sfu` 的 NACK/FEC 单测（步骤 3）断言重传与冗余恢复的**数据面正确性**；视觉主观项未覆盖 |
| 2 | 硬件加速后 1080p CPU 降低 ≥60% | 未覆盖 | 需要 GPU 环境与基线；CI runner 无 GPU |
| 3 | 单服务器 ≥200 路 720p | 未覆盖 | 需要长时压测与真实视频源；`bench/benchmark.sh` 是手工入口 |
| 4 | 端到端延迟 ≤200ms | 未覆盖 | 需要端到端测量探针，当前仓库未实现 |

### QM-004 信令协议

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | WebSocket 完成 SDP/ICE 协商 | 未覆盖 | 信令 crate 已实现 HTTP + `/healthz`，WebSocket 分支与浏览器端未交付 |
| 2 | 首次连接成功率 ≥99% | 未覆盖 | 需要跨机压测 |
| 3 | 无效 JWT 100% 被拦截 | 半自动 | 信令单测里覆盖鉴权分支（步骤 3）；「无法获取任何会议信息」需人工核查响应体 |
| 4 | 并发 1000 信令连接 | 未覆盖 | 需要压测环境 |

### QM-005 房间管理

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | 带密码会议、错误密码拒绝 | 未覆盖 | 该功能尚未交付 |
| 2 | 等候室与主持人审批 | 未覆盖 | 同上 |
| 3 | 主持人操作 100ms 内同步 | 未覆盖 | 同上 |
| 4 | 最后离开 5 分钟释放资源、无泄漏 | 半自动 | `qm-common` 的存储/超时单测（步骤 3）覆盖超时释放逻辑；「无内存泄漏」需长时运行观察 |

### QM-006 分布式集群（本次 PR 直接相关）

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | 3 节点部署、会议分配到不同节点 | 自动 | `compose-legacy` 步骤 8：`qm-media`/`qm-media-2`/`qm-media-3` 三节点 + NATS 全部 `healthy`；最低负载调度的分配决策由 `qm-cluster` 单测断言 |
| 2 | 节点宕机 10 秒内迁移 | 半自动 | 判死窗口 5s×2=10s 由 `qm-cluster` 单测直接断言（`heartbeat_secs × unhealthy_misses == 10`）；真机故障注入（kill 一个容器看迁移）未自动化 |
| 3 | 单会议 ≥10000 人旁听 | 半自动 | 旁听容量口径 `listener_weight() ≥ 10000` 由单测断言；真实 1 万人旁听需要压测 |
| 4 | 新节点 30 秒内接入 | 半自动 | `join_target_secs == 30` 由单测断言；真机新加入需要手工验 |

### QM-015 容器化部署（本次 PR 直接相关）

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | `docker-compose up -d` 后 5 分钟内全部启动成功 | 自动 | `compose-legacy` 步骤 8（脚本给 240s 预算，比 5 分钟更严） |
| 2 | 健康检查正常、无启动失败、无循环重启 | 自动 | 步骤 8（五容器 `healthy`，重启次数 >6 判失败）+ 步骤 9（逐个 restart 后必须回到 `healthy`） |
| 3 | 网页端访问/建会/入会 | 未覆盖 | 前端未交付（QM-012） |
| 4 | 改配置重启即生效 | 半自动 | `qm-common` 配置加载单测（步骤 3）覆盖 `default.toml` → `local.json` → `QM_*` 的覆盖顺序与 fail-fast；「重启即生效」端到端需手工验 |

### QM-018 本 Issue（PR/CI 验收链路）

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | 远端仓库可见、4 个提交与分支可 push、PR 可创建并关联 Issue | 半自动 | push 与 PR 创建是一次性人工动作（本次交付已做）；`pr-title` job 自动守住标题约定，标题带编号 Issue 才会自动关联 |
| 2 | ubuntu 上 `cargo test --workspace` 全绿 + docker-compose 构建并健康检查通过 | 自动 | `rust-msrv` 步骤 3 + `compose-legacy` 步骤 6/7/8 |
| 3 | windows-latest 上 `cargo test -p qm-media --features webrtc` 通过 | 自动 | `windows-webrtc`（先断言 `cl.exe` 存在，避免「静默跳过」假绿） |
| 4 | 每个 QM 验收项能指到 CI job 或本地命令 | 自动 | 就是本文档第 2 节 |
| 5 | 单次 CI 运行 ≤15 分钟 | 自动 | 每个 job 的 `timeout-minutes`；实际耗时看 run 详情（首次冷缓存可能接近上限） |

> QM-007 ~ QM-014、QM-016 ~ QM-017、QM-019 ~ QM-022 尚未交付代码，
> 它们的验收项当前**全部未覆盖**，等对应 Issue 交付后再回到本表补一行。
> 补规则：新增验收项必须同时给出「自动 / 半自动 / 未覆盖」三档之一的判定与具体 job 或命令。

## 3. 本表自身的维护约定

1. 新增或修改 `QM-0xx` 的验收标准时，**同一个 PR 内**更新本表，否则 `pr-title` 之外的
   review 环节会打回（文档滞后与代码不一致是 Epic 明确禁止项）。
2. 把某项从「未覆盖」升级到「自动」时，要同时给出能挡住回归的失败条件
   （例如「重启次数 >6 判失败」），不要只写「跑了 xxx」。
3. 「未覆盖」项必须写清本地命令或明确写「当前无法机器验证」，禁止留空。
4. 表格里引用的 job 名改了就同步改 CI 文件，两处必须一致。

## 4. 已知缺口（本次交付未覆盖）

- **分支保护未开启**：`.github` 下的配置需要仓库 admin 权限。请用
  `gh api --method PUT /repos/yejinlei/QuickMeet/branches/main/protection` 开启，
  或直接到 Settings → Branches 设置：要求 `rust-msrv`、`compose-legacy`、
  `windows-webrtc` 三个 job 通过后才能合并。
- **首次 CI 运行时长未经实测**：冷缓存下载 + Dockerfile 首次编译可能接近 15 分钟上限，
  第一次跑完后可把 `rust-cache` 的 key 收紧以稳定耗时。
- **浏览器/端到端类验收（QM-001-2、QM-002 全部、QM-003-1 视觉项等）未覆盖**：
  仓库目前没有前端与浏览器测试基座（`ecc:e2e-runner` / Playwright 均可），
  等 QM-012 交付前端后再接入。
- **workspace 全量 clippy 仍有历史 warning**（`qm-media` / `qm-sfu` / `qm-signaling`），
  是历史提交遗留的技术债。本 PR 只把 `qm-common` / `qm-cluster` 这两个改动范围内的
  crate 挡到零 warning；workspace 全量的结果由 `rust-msrv` 步骤 2c 打印出来，
  不阻塞合并。清理完历史 warning 后把 `scripts/verify.sh` 步骤 2b 的
  `-p qm-common -p qm-cluster` 改成 `--workspace` 即可。
- **`windows-webrtc` 的 MSVC 编译路径只有 CI 能验**：本机只有 MinGW 工具链，
  切到 `stable-x86_64-pc-windows-msvc` 时 proc-macro crate 的 host/target 不匹配
  （`cl.exe` 不在），所以这条验收在本地无法预演，首次结果要等 CI。
