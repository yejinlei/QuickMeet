# QuickMeet 私有化部署手册（QM-015）

一条命令拉起全套：

```bash
docker-compose up -d --build      # 1.29.2 的 CLI，不是 docker compose
```

或用一键脚本（做了前置检查 + 五容器 healthy 等待 + 探活）：

```bash
bash scripts/qm-up.sh
```

| 操作 | 命令 |
| --- | --- |
| 启动 | `bash scripts/qm-up.sh`（等价 `docker-compose up -d --build`） |
| 升级 | `bash scripts/qm-upgrade.sh`（重建 + 逐个滚动重启，数据卷不动） |
| 停止 | `bash scripts/qm-down.sh`（保留数据卷） |
| 清数据 | `bash scripts/qm-down.sh --purge`（删卷，输入 `PURGE` 二次确认） |
| 看日志 | `docker-compose logs -f qm-media` |
| 探活 | `curl http://<host>:8081/healthz`（信令）、`:8091/8092/8093`（三个媒体节点） |

## 1. 服务与端口

| 服务 | 容器内端口 | 宿主机端口 | 说明 |
| --- | --- | --- | --- |
| `qm-nats` | 4222 / 8222 | 4222 / 8222 | 集群协调点（core NATS，无 JetStream） |
| `qm-media` | 8080 / 8090 | 8080 / 8091 | 媒体节点 1（node-1），8090 是健康探针 |
| `qm-media-2` | 8080 / 8090 | 8082 / 8092 | 媒体节点 2（node-2） |
| `qm-media-3` | 8080 / 8090 | 8083 / 8093 | 媒体节点 3（node-3） |
| `qm-signaling` | 8081 | 8081 | 信令服务 |

### 地址模型：advertised_addr 写宿主机地址，不是容器地址

容器网络用 Docker 默认子网（172.17.0.0/16），**刻意不写** `ipam.config` 把子网
设成 192.168.0.0/24。原因：`QM_CLUSTER_ADVERTISED_ADDR` 里的地址要同时被
「内网里的浏览器」和「其他容器」访问到，单机部署下只有**宿主机 IP + 端口映射**
能同时满足两边。容器子网若也设成 192.168.0.0/24，容器 IP 和宿主机 LAN IP 会
撞成同一个 192.168.0.10，浏览器拿到 advertised_addr 后到底连谁就不可预测了。

所以三个节点写的是宿主机侧地址（宿主机内网 IP + 映射出的端口）。其他容器访问
这个地址会走宿主机的 Docker DNAT 绕一圈（hairpin），功能正确，只是多一跳，
集群判活（10s 窗口）不受影响。

> **现场改法**：宿主机内网 IP 不是 192.168.0.10 时，把三个节点的
> `QM_CLUSTER_ADVERTISED_ADDR` 的 IP 段改成宿主机实际 IP（端口保持不变）。
> 若需要把媒体节点绑定到具体的宿主机网卡，改 `QM_NETWORK_BIND_HOST` 与
> `ports:` 映射即可。
>
> 多机部署时每台宿主机填自己的内网 IP，节点之间就能直接互通，不走 hairpin。

## 2. 镜像来源（全部官方基础镜像）

| 服务 | 镜像 | 说明 |
| --- | --- | --- |
| `qm-*` | 本仓库 `Dockerfile` 构建 | 构建阶段 `rust:1.75`（官方），运行阶段 `debian:bookworm-slim`（官方） |
| `qm-nats` | `nats:2.10.21-alpine` | NATS 官方镜像，**pin 到 patch 版本**不用 tag |

运行阶段与编译解耦后镜像只有几十 MB（编译工具链不进镜像）。`nats` 镜像固定 patch
版本是刻意的：私有化部署要能预拉取、要可复现，不依赖 Docker Hub 上 tag 的漂移。

断网环境：先把这两个镜像导成 tar 预置（`docker save` / `docker load`），
Rust 依赖全部来自 crates.io（`Cargo.lock` 已钉版本），CI 里 `--locked` 构建可复现。

## 3. 配置（改配置重启即生效，验收标准 4）

三层覆盖，后者覆盖前者：

1. `config/default.toml` —— 代码里的默认值
2. `config/local.json` —— 可选，**存在才加载**
3. 环境变量 `QM_` 前缀 —— `config/container.env` 由 compose 的 `env_file` 注入

**只按第一个 `_` 分层**：`QM_MEDIA_SIGNALING_PORT` → `media.signaling_port`
（不是 `media.signaling.port`）。写错字段名会被**直接拒绝启动**，不会静默忽略。
`Vec` 字段（如 `network.cidrs`）只能用 JSON 数组，索引写法会 fail fast。

### 常见现场调整

| 需求 | 改哪里 | 之后 |
| --- | --- | --- |
| 换端口 | `config/container.env` 的 `QM_MEDIA_PORT` / `QM_MEDIA_SIGNALING_PORT`，并同步改 compose 的 `ports:` 映射 | `docker-compose up -d` |
| 改内网网段 | 只改 `QM_NETWORK_CIDRS`（JSON 数组，**不改容器网络**）；同时把三个节点的 `QM_CLUSTER_ADVERTISED_ADDR` 改成新网段下的宿主机 IP | `docker-compose up -d`（容器子网保持 172.17.0.0/16 不动，所以不需要重建网络；原因见上面「地址模型」） |
| 调日志级别 | `QM_LOGGING_LEVEL`（注意：逗号后**不能有空格**，否则后面的 target 被静默丢弃） | `docker-compose up -d` |
| 单节点会议上限 | `QM_CLUSTER_MAX_ROOMS_PER_NODE` | `docker-compose up -d` |
| 宿主机内网 IP 不是 192.168.0.10 | 把三个节点的 `QM_CLUSTER_ADVERTISED_ADDR` 的 IP 段改成宿主机实际 IP（端口映射保持不变） | `docker-compose up -d` |
| 加一台媒体节点 | 复制一份 `qm-media-3` 段，改 `node_id` / `advertised_addr` / `ports:` 映射与数据卷名（**不需要**固定 IP，容器走默认子网） | `docker-compose up -d` |

改完 `config/container.env` **不需要重新构建镜像**：env_file 是运行时注入的，
`docker-compose up -d` 会用新值重启容器。真正需要 `--build` 的只有代码或
`config/default.toml` 的改动（default.toml 被 COPY 进镜像）。

> **不要**把 `./config/local.json` 挂进容器。文件不存在时 Docker 会创建同名
> **目录**，配置加载直接异常。现场需要 local.json 覆盖时，把它拷进镜像或走
> 环境变量。

## 4. 集群与故障迁移

三个媒体节点共用一个 NATS，靠 `QM_CLUSTER_NODE_ID` / `QM_CLUSTER_ADVERTISED_ADDR`
区分自己。判死窗口 = `heartbeat_secs × unhealthy_misses` = 5 × 2 = **10s**，与
docker healthcheck 的 `interval 5s × retries 2` 用同一个口径 —— 容器判死和集群
判死不会互相打架。

扩容不需要重建镜像，也不需要重启现有节点：复制一份 `qm-media-*` 服务、改身份与
端口映射即可。旁听分发节点用 `docker-compose run` 起（见 compose 文件末尾的示例），
`node_role=listener` 时不参与调度，只承接只收流转发。

## 5. AI 服务接入（QM-013，服务未交付）

当前 `QM_AI_ENABLED=false`，仓库里没有 AI 服务的代码，compose 里也**没有**
`qm-ai` 容器。接入步骤：

1. 在局域网起本地部署的硅基流动服务（**禁止公网第三方 API**，Epic 全局约束 4）；
2. 解开 `docker-compose.yml` 文件头注释里的 `qm-ai` 段，`image` 必须 pin 官方镜像的
   patch 版本；
3. 把 `QM_AI_BASE_URL` 指向容器可达地址（`127.0.0.1` 在容器里只指容器自己，
   要用宿主机 IP 或 `host.docker.internal`）；
4. `QM_AI_ENABLED=true`，`docker-compose up -d`。

## 6. 未交付的服务（不要凭空起容器）

按 Issue 要求，未落地的服务在 compose 里留了**注释占位**并标明扩展位，
本仓库当前只有 `qm-demo` 一个二进制：

- **SFU**：`qm-sfu` crate 已实现，但**没有任何进程入口**（QEJ-91 遗留），
  所以没有独立 SFU 镜像。等 QM-002 的入口交付后解开 `qm-sfu` 占位。
- **TURN（QM-008）**：当前所有 ICE 候选都是 host / prflx / srflx，**没有 relay
  路径**，配置层也没有对应键。`config/container.env` 里 `QM_ICE_TURN_URL` 是
  注释掉的 —— 现在赋真值会被判成未知配置项直接拒绝启动。
- **前端（QM-012）**：没有前端产物，所以「网页端可访问、创建会议、入会互通」
  这条验收**当前无法自证**。仓库里只有 `web/interop/index.html`（人工核验页面）。
- **JWT / TURN 凭据 / ICE 密钥**：配置层没有这些键，所以交付内容里的
  「密钥」目前是**无操作项**。QM-004（信令鉴权）交付后一并补
  `config/container.env` 与 compose 的 `environment:` 段。

## 7. 合规自查（Epic 全局约束）

- 数据不出域：会议数据落 `meeting-data*` named volume，日志落 `meeting-logs`，
  都是本地 named volume；容器只监听内网网段，没有出站公网调用路径。
- 媒体服务默认 8080：`config/default.toml` 的 `media.port = 8080`，
  `scripts/verify.sh` 步骤 5b 有断言。
- 容器非 root 运行：Dockerfile 里 `USER 1000`。
- 依赖全部来自 crates.io：`Cargo.lock` 已钉版本，没有 `[patch.crates-io]`
  指向 git / 本地路径（那样会破坏断网部署的可复现性）。
