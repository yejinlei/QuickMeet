# QuickMeet CI 验收矩阵（QM-018）

对应 Issue **QEJ-109 / QM-018**。目的：每条 QM 验收标准都能指到**具体的 CI job**
或**具体的本地命令**，不再出现「这条怎么验没人说得清」。

## 0. CI job 一览

| Job id → check-run 显示名 | 运行环境 | 做什么 | 超时 |
| --- | --- | --- | --- |
| `rust-msrv` → **ubuntu / Rust 1.75 / workspace 全绿** | ubuntu-22.04 + Rust **1.75** | MSRV 检查、`fmt --check`、`clippy`（本 PR 触碰的 crate 硬挡 `-D warnings`；workspace 全量只作提示）、`cargo test --workspace --locked`、`cargo build --release`、`qm-demo` 默认命令 + 端口假设 | 15 min |
| `compose-legacy` → **ubuntu / docker-compose 1.29.2 / 容器健康检查** | ubuntu-22.04 + docker + **docker-compose 1.29.2** | Dockerfile 构建、compose 语法与 1.29.2 兼容校验、`up -d --build`、五容器健康检查、`/healthz` 探活、逐个 restart 自愈验证 | 15 min |
| `windows-webrtc` → **windows-latest / MSVC / qm-media --features webrtc** | windows-latest + MSVC `cl.exe` | `cargo test -p qm-media --features webrtc --locked`（本机只有 MinGW，跑不到这一条） | 15 min |
| `pr-title` → **PR 标题 QM-00x: xxx 约定** | ubuntu-22.04 | PR 标题必须匹配 `^(QM-\d{3}\|YEJ-\d+): .+`，固化标题约定并保证 Issue 自动关联 | 5 min |
| `workflow-lint` → **workflow 语义校验（YEJ-114）** | ubuntu-22.04 + python3 | 跑 `scripts/verify-workflows.sh`（与本地预检**同一份脚本**）：校验所有 workflow 的 YAML 语义陷阱、job 结构、`uses:` 依赖来源与固定 ref、`timeout-minutes ≤ 15`；脚本末尾再跑 7 个反例自证。**硬门禁只有这一步**；之后追加一个非阻塞的第三方 `mpalmer/action-validator@v0.9.0` 交叉校验（`continue-on-error`），见第 3 节 | 5 min |

> 本表最后一列的超时与 `ci.yml` 里各 job 的 `timeout-minutes` 一致；改任一处都要同步改另一处。

> **两个名字别混用**：`rust-msrv` 这类是 workflow 里的 `jobs.<id>`，
> 只在 `needs:` / `${{ }}` 表达式里有效；**分支保护的
> `required_status_checks.contexts` 与 `gh pr checks` 认的是右边那个显示名**
> （`jobs.<id>.name`，没写 `name` 时才退回 id）。id 里原来带点号（`rust-1.75`、
> `compose-1.29.2`）时 Actions 会把它当成 JSONPath 报
> `Can get expression value only for Object or Array, got: 'Number'`，
> 整个 workflow 直接不执行 —— 所以 id 已经改成无点号，显示名保留完整描述。

约束达成情况（Issue 强制约束）：

- **无私有凭据**：全部工件来自 crates.io、Docker Hub、GitHub 官方 release；
  workflow 里没有任何 token 字面量，`permissions: contents: read`。
- **无第三方 SaaS**：不接监控/构建服务，只有 GitHub Actions 自身。
- **compose 1.29.2**：CI 里装的是**真实 1.29.2 二进制**（不是 `docker compose`
  v2 语法校验），并且 `verify.sh` 步骤 7 额外做 2.x 专属键**分级**黑名单扫描：
  顶格（顶层）查 `deploy` / `extends` / `develop` / `secrets` / `config` / `include`，
  服务级（1~4 空格缩进）查 `extends` / `develop`。刻意**不查**服务级 `secrets`
  （1.29.2 合法键）、`ipam.config`（1.29.2 支持 `networks.<n>.ipam.config`）与
  build 的 `target`（多阶段 build 目标选择，1.29.2 支持）。
- **NATS 镜像 tag 存在性**：`compose-legacy` 里单独一步 `docker pull
  nats:2.10.21-alpine` + `docker image inspect`。1.29.2 拉不到镜像会一直卡在
  拉取阶段，表象是「五容器起不来」而不是明确报错 —— 提前 pull 一次让失败
  发生在能看出是哪一层问题时（同时预热镜像缓存）。
- **≤15 分钟**：每个 job 单独 `timeout-minutes`，最长的 `compose-legacy` 是 15。

## 1. 本地一键验证

```bash
bash scripts/verify.sh                # 全部 10 步（步骤 6-9 需要 docker + docker-compose）
bash scripts/verify.sh --no-docker    # 跳过容器步骤（本机没有 Docker 时用；步骤 10 照跑）
STEPS="1 3 4 5" bash scripts/verify.sh --no-docker   # 只跑指定步骤
STEPS=10 bash scripts/verify.sh --no-docker          # 只跑 workflow 校验（最快，秒级）
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
| 7 | compose 1.29.2 兼容**分级**黑名单扫描（顶层 `deploy`/`extends`/`develop`/`secrets`/`config`/`include`；服务级 `extends`/`develop`）+ `docker-compose config` | `compose-legacy` |
| 8 | `docker-compose up -d --build` + 五容器全部 `healthy` + 四个 `/healthz` 返回 200 + **8c 集群硬断言**（逐个节点 `nats_connected == true` 且 `cluster_nodes >= 3`） | `compose-legacy` |
| 9 | 逐个 `docker restart`，轮询最多 120s 等状态收敛，验证 `restart: unless-stopped` 自愈、无循环重启 | `compose-legacy` |
| 10 | `scripts/verify-workflows.sh`：workflow YAML 语义校验 + 7 个反例自证（不依赖 docker，`--no-docker` 时也跑） | `workflow-lint` |

> 步骤 8 里重启次数 >6 次就判定「循环重启」并失败；这是 QM-015 验收标准 2 的机器化判据。
>
> `STEPS` 的选择是按**整数字符串精确匹配**（数组比较），不是子串匹配。
> 旧的写法 `case " $STEPS " in *" 1 "*` 会让 `want 1` 与 `want 10` 互相命中
> （尾部空格使 `" 1 "` 成为 `" 10 "` 的前缀），加第 10 步后才暴露这个问题。

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
| 1 | 3 节点部署、会议分配到不同节点 | 自动 | `compose-legacy` 步骤 8：**8c 集群硬断言** —— 三个媒体节点逐个断言 `/healthz` JSON 里 `nats_connected == true` 且 `cluster_nodes >= 3`，任一退化单机就失败；最低负载调度的分配决策由 `qm-cluster` 单测断言。没有 8c 之前这条只验到「五容器 healthy」，NATS 挂了照样全绿 —— 那是假通过 |
| 2 | 节点宕机 10 秒内迁移 | 半自动 | 判死窗口 5s×2=10s 由 `qm-cluster` 单测直接断言（`heartbeat_secs × unhealthy_misses == 10`）；真机故障注入（kill 一个容器看迁移）未自动化 |
| 3 | 单会议 ≥10000 人旁听 | 半自动 | 旁听容量口径 `listener_weight() ≥ 10000` 由单测断言；真实 1 万人旁听需要压测 |
| 4 | 新节点 30 秒内接入 | 半自动 | `join_target_secs == 30` 由单测断言；真机新加入需要手工验 |

### QM-015 容器化部署（本次 PR 直接相关）

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | `docker-compose up -d` 后 5 分钟内全部启动成功 | 自动 | `compose-legacy` 步骤 8（脚本给 240s 预算，比 5 分钟更严） |
| 2 | 健康检查正常、无启动失败、无循环重启 | 自动 | 步骤 8（五容器 `healthy`，重启次数 >6 判失败）+ 步骤 9（逐个 restart 后必须回到 `healthy`）。**`healthy` 是真的有意义**：`/healthz` 在 NATS 未连接时返 503，docker healthcheck 的 `curl -f` 会判失败，不会出现「五容器全绿但集群死了」的假通过。启动窗口（`start_period 90s`）与判死窗口（`interval 5s × retries 3` ≈ 10s）是解耦的两个数 |
| 3 | 网页端访问/建会/入会 | 未覆盖 | 前端未交付（QM-012） |
| 4 | 改配置重启即生效 | 半自动 | `qm-common` 配置加载单测（步骤 3）覆盖 `default.toml` → `local.json` → `QM_*` 的覆盖顺序与 fail-fast；「重启即生效」端到端需手工验 |
| 5 | （并入 YEJ-114）新增/修改 workflow 前必须本地过预检 | 自动 | 步骤 10 / `workflow-lint`：`scripts/verify-workflows.sh` 校验 YAML 语义陷阱、job 结构、`uses:` 依赖来源与固定 ref、`timeout-minutes ≤ 15`，并在末尾跑 7 个反例自证 |

### QM-018 本 Issue（PR/CI 验收链路）

| # | 验收项 | 覆盖 | 由谁覆盖 |
| --- | --- | --- | --- |
| 1 | 远端仓库可见、4 个提交与分支可 push、PR 可创建并关联 Issue | 半自动 | push 与 PR 创建是一次性人工动作（本次交付已做）；**PR 标题 QM-00x: xxx 约定** job 自动守住标题约定，标题带编号 Issue 才会自动关联 |
| 2 | ubuntu 上 `cargo test --workspace` 全绿 + docker-compose 构建并健康检查通过 | 自动 | `rust-msrv` 步骤 3 + `compose-legacy` 步骤 6/7/8 |
| 3 | windows-latest 上 `cargo test -p qm-media --features webrtc` 通过 | 自动 | **windows-latest / MSVC / qm-media --features webrtc**（先断言 `cl.exe` 存在，避免「静默跳过」假绿） |
| 4 | 每个 QM 验收项能指到 CI job 或本地命令 | 自动 | 就是本文档第 2 节 |
| 5 | 单次 CI 运行 ≤15 分钟 | 自动 | 每个 job 的 `timeout-minutes`；实际耗时看 run 详情（首次冷缓存可能接近上限） |
| 6 | （并入 YEJ-114）workflow 自身的语义与依赖校验，不引入私有依赖 | 自动 | `workflow-lint` job + 本地步骤 10 跑同一份 `scripts/verify-workflows.sh`：`uses:` 走 owner 白名单（`actions` / `actions-rs` / `dtolnay` / `swatinem` / `ilammy` / `mpalmer`）且必须 pin tag 或 SHA，禁止本地路径与分支名。硬门禁部分只依赖 `actions/checkout@v4`；新增的第三方 action 是**非阻塞**交叉校验，见第 3 节末 |

> QM-007 ~ QM-014、QM-016 ~ QM-017、QM-019 ~ QM-022 尚未交付代码，
> 它们的验收项当前**全部未覆盖**，等对应 Issue 交付后再回到本表补一行。
> 补规则：新增验收项必须同时给出「自动 / 半自动 / 未覆盖」三档之一的判定与具体 job 或命令。

## 3. workflow 编写约定（YEJ-114 并入 QM-015）

> **硬规则：新增或修改 `.github/workflows/*.yml` 的 PR，合并前必须本地跑过
> `bash scripts/verify-workflows.sh` 且退出码 0。**
> 这条由 CI job `workflow-lint` 自动兜底（它与本地预检是**同一份脚本**，
> `scripts/verify-workflows.sh`，硬门禁不依赖任何第三方 action），但本地先跑能
> 省一轮失败的 CI 排队。校验内容：YAML 语义陷阱、job 结构（`runs-on` /
> `timeout-minutes` / `steps`）、`uses:` 依赖来源与固定 ref、`timeout-minutes ≤ 15`。
> 脚本末尾还会跑 7 个反例自证，用来证明**校验器本身没失效**。
>
> CI job 末尾**追加**了一个第三方校验器（`mpalmer/action-validator@v0.9.0`）做
> 交叉验证，`continue-on-error: true` —— 它是补充信号，不是门禁，追加不是替换。

### 三个真实踩过的坑与修法

| 坑 | 触发形态 | 表象 | 修法 |
| --- | --- | --- | --- |
| `on:` 被 YAML 1.1 吞成布尔 | 顶层 `on:` 不加引号（**这是 Actions 的标准写法**） | 用 pyyaml / yq / 部署脚本读这个文件时，顶层键是 Python 的 `True` 而不是 `"on"` | **不要**因此把 `on:` 改成 `"on":`。Actions 有自己的解析器，不受 YAML 1.1 影响，workflow 照常运行；被坑的是任何消费 pyyaml 输出的工具。所以校验器**不能**断言「解析结果必须有 `on` 键」（那样每个正常的 Actions workflow 都会误报），而是做**文本层 vs 解析层对账**：把文本里的顶层键与解析后的键集合比一下，差集必须全部落在 YAML 1.1 的 bool 字面量集合 `{on, off, yes, no, true, false, y, n}` 里；出现集合外的差集键才是真正的事故 |
| job id 含点号 | `rust-1.75`、`compose-1.29.2` | Actions 把 job id 放进表达式上下文按 JSONPath 解析，点号被当成取字段，报 `Can get expression value only for Object or Array, got: 'Number'`，**整个 workflow 静默不执行** —— 表象是「CI 没跑」，不是语法错误 | job id 只用 `[A-Za-z0-9_-]`，版本号留在 `name:` 里给人看（如 id `rust-msrv` + name `ubuntu / Rust 1.75 / workspace 全绿`）。校验器断言 id 字符集 |
| `name:` 含「冒号+空格」没加引号 | `name: PR 标题 QM-00x: xxx 约定` | YAML 把它当成 inline mapping，要么解析报错，要么 `name` 解析成 dict | 含「冒号+空格」时给整个值加引号：`name: "PR 标题 QM-00x: xxx 约定"`。校验器断言 job 级与 step 级的 `name` 解析出来都是字符串 |

### 其他约定（同样由 `workflow-lint` 机器判据兜住）

1. 每个 job 必须有 `runs-on`、`timeout-minutes`（且 ≤ 15 分钟，Issue 约束的 CI 预算上限）、
   非空 `steps`；每个 step 必须有 `uses` 或 `run`。
2. `uses:` 只允许**公开仓库 + 固定 ref**（tag 或提交 SHA）。禁止本地路径（`./`）、
   `docker://`、无 ref（会解析到默认分支 HEAD 而漂移）、分支名（`main` / `master`）。
   owner 走白名单 —— 白名单里的都是社区公开 action；**加一个 owner 等于引入新的
   供应链依赖，必须在 PR 里写明理由**。
3. 顶层 `permissions:` 必须显式声明最小权限（缺了直接判失败，不是提示）；
   `concurrency:` 建议声明（缺了只提示）。
4. `workflow-lint` job 的**硬门禁**只有 `scripts/verify-workflows.sh`，它的依赖
   只有 `actions/checkout@v4`。末尾追加的 `mpalmer/action-validator@v0.9.0` 是
   `continue-on-error` 的**补充交叉校验**，不是门禁，替换自研校验的话会丢
   YAML 1.1 语义那类坑的机器判据。
   复审意见里写的「官方 `actions/action-validator`」**不是真实 ref**：
   `GET https://api.github.com/repos/actions/action-validator` 返回 404
   （同批对照的 `actions/checkout` 等仓库返回 200，已排除限流误判）。
   `action-validator` 这个名字的真实归属是 `mpalmer/action-validator`
   （另一个同类工具是 `rhysd/actionlint`，更常用，未采用是因为它需要自己
   下载二进制、参数契约面更大），白名单里加的是 `mpalmer` 而不是往 `actions/*`
   里凑一个假 ref。
   选非阻塞而不是硬门禁的理由：这是第三方 action，每次都在 CI 上现下载一个
   二进制，上游一动就红，而它带来的增量收益远小于自研校验。

## 4. 本表自身的维护约定

1. 新增或修改 `QM-0xx` 的验收标准时，**同一个 PR 内**更新本表，否则 **PR 标题 QM-00x: xxx 约定** 之外的
   review 环节会打回（文档滞后与代码不一致是 Epic 明确禁止项）。
2. 把某项从「未覆盖」升级到「自动」时，要同时给出能挡住回归的失败条件
   （例如「重启次数 >6 判失败」），不要只写「跑了 xxx」。
3. 「未覆盖」项必须写清本地命令或明确写「当前无法机器验证」，禁止留空。
4. 表格里引用的 job 名改了就同步改 CI 文件，两处必须一致。
5. 改 `.github/workflows/*.yml` 时同步跑第 3 节的本地预检（见该节的硬规则）。

## 5. 已知缺口（本次交付未覆盖）

- **分支保护已开启**（`main` 分支，QM-018 交付时已 PUT）。注意 contexts 必须填
  **check-run 显示名**而不是 job id —— 填成 job id（`rust-msrv` 等）时保护规则
  对不存在的状态永远卡住、也永远放不了行。开启命令：
  `gh api --method PUT repos/yejinlei/QuickMeet/branches/main/protection`（`-f` 会把 JSON
  字符串化，用 `--input <file>`；`restrictions` 必须给，个人仓库传 `null`）。
  QM-018 交付时配置的是 4 条 contexts：`ubuntu / Rust 1.75 / workspace 全绿`、
  `ubuntu / docker-compose 1.29.2 / 容器健康检查`、
  `windows-latest / MSVC / qm-media --features webrtc`、`PR 标题 QM-00x: xxx 约定`。

  ⚠️ **本 PR 新增了第 5 个 job `workflow-lint`（显示名 `workflow 语义校验（YEJ-114）`），
  分支保护需要重新 PUT 一次把这条 context 加进去** —— contexts 不会自动跟随 workflow 变化。
  注意括号是全角的，`required_status_checks.contexts` 必须逐字符匹配显示名，
  填成英文括号 `()` 会挂不到任何状态、把 `main` 卡死。
  本 PR 没有擅自 PUT（分支保护是仓库级设置，需要人工确认），由 reviewer / 仓库维护者
  在合并前或合并后补这一次 PUT。
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
- **`windows-webrtc` job（显示名 windows-latest / MSVC / qm-media --features webrtc）的
  MSVC 编译路径只有 CI 能验**：本机只有 MinGW 工具链，
  切到 `stable-x86_64-pc-windows-msvc` 时 proc-macro crate 的 host/target 不匹配
  （`cl.exe` 不在），所以这条验收在本地无法预演，首次结果要等 CI。
- **第三方 action 的运行时契约未在本机实测**：`mpalmer/action-validator@v0.9.0`
  已在 GitHub API 层面确认仓库存在、非归档、tag `v0.9.0` 存在（本地无 Actions
  runner，跑不了 `runs.using: composite` 的实际执行）。它是 `continue-on-error`
  的补充校验，失败不阻塞合并；首次 CI 跑完看一下这一步的日志，确认它扫到的是
  `.github/workflows/*.yml*` 且没有因 `gh release download` 缺权限而失败
  （文件顶部已给 `permissions: contents: read`）。
