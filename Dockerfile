# QuickMeet 运行镜像（QM-015）
#
# 构建阶段用 rust:1.75（官方镜像）编译 release，运行阶段用 debian:bookworm-slim
# （官方镜像）只放二进制与配置。两个基础镜像都来自 Docker Hub 官方命名空间，
# 不引入任何不明来源镜像，也不带任何 tag 漂移风险 —— 见 docs/DEPLOYMENT.md 的
# 「镜像来源」一节。
#
# MSRV 1.75 与 Issue 全局约束 1 对齐；运行阶段与编译解耦后镜像从 ~1.2 GB 降到
# 几十 MB（Issue 交付要求「多阶段构建精简镜像体积」）。
FROM rust:1.75 AS builder

# rust:1.75 已是 slim 变体，不带 gcc。ring（async-nats 默认 features 的传递依赖，
# rustls → ring）的 build.rs 需要它，proc-macro crate 也需要链接器。
# curl / wget 留给运行阶段的 docker healthcheck（见 docker-compose.yml）。
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl wget \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /opt/quickmeet

# 先复制清单，利用层缓存：依赖没变时不用重新下载 crates.io。
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY crates crates
COPY demos demos

RUN cargo build --release --locked

# ── 运行阶段 ────────────────────────────────────────────────────────
# debian:bookworm-slim 是 rust:1.75-slim 的基座同代镜像，glibc 版本一致，
# 静态检查过的 release 二进制直接可跑，不需要带任何 Rust 工具链进镜像。
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl wget \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    # 1000 用户在 slim 镜像里不一定存在，显式建一个非 root 运行身份。
    && groupadd --system --gid 1000 app \
    && useradd --system --uid 1000 --gid app --create-home --home-dir /home/app app

WORKDIR /opt/quickmeet

# 只拷二进制，不带 target 目录（调试符号、增量编译中间产物都不进镜像）。
COPY --from=builder /opt/quickmeet/target/release/qm-demo ./qm-demo
COPY config config
RUN mkdir -p /opt/quickmeet/data /opt/quickmeet/logs \
    && chown -R 1000:1000 /opt/quickmeet \
    && chmod +x /opt/quickmeet/qm-demo

# 非 root 运行（容器安全基线）。
USER 1000

# 媒体服务默认监听 8080（Epic 全局约束），信令 8081，集群健康探针 8090。
EXPOSE 8080 8081 8090

# 默认跑编解码收发验证；带 --signal 起信令服务，带 --cluster 进集群模式。
# 集群参数由 docker-compose.yml 各服务显式传（只有 CMD、没有 ENTRYPOINT，
# 所以 command 必须是完整命令行字符串），CMD 只是 `docker run` 的默认值。
CMD ["./qm-demo", "--bind", "0.0.0.0"]
