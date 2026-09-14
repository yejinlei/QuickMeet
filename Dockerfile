# QuickMeet Stage 1 镜像
#
# 基于 Rust 1.75 官方镜像 —— 与验收标准 1 的 MSRV 要求一致。
# 默认构建**不启用**任何 C 工具链相关的依赖：编解码是纯 Rust 确定性封装，
# webrtc-rs（需要 ring 的 C 编译）是 optional feature，默认不编译。
FROM rust:1.75-slim

# slim 镜像不带 gcc，而 Cargo 构建 proc-macro / 少量 -sys crate 时需要一个 C 链接器。
# 这里只做最小安装，不在镜像里装 MSVC / Visual Studio Build Tools。
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /opt/quickmeet

# 先复制清单，利用 Docker 层缓存：依赖没变时不用重新下载 crates.io。
COPY Cargo.toml Cargo.lock ./
COPY .cargo .cargo
COPY crates crates
COPY demos demos

# release 构建。--locked 保证与仓库里的 Cargo.lock 一致。
RUN cargo build --release --locked

# 拷入配置模板（不含 local.json，那是现场覆盖，应通过 volume 挂载）。
COPY config config
RUN mkdir -p /opt/quickmeet/data /opt/quickmeet/logs \
    && chown -R 1000:1000 /opt/quickmeet

# 非 root 运行（容器安全基线）。
USER 1000

# 媒体服务默认监听 8080（Epic 全局约束），信令 8081。
EXPOSE 8080 8081

# 默认跑编解码收发验证；带 --signal 才起信令服务，带 --cluster 进集群模式。
# 集群参数由 docker-compose.yml 的 media 服务显式传（见那边的 command:），
# 不写在这里 —— CMD 只是默认值，三个节点必须各自带 --cluster 才会互认识。
CMD ["./target/release/qm-demo", "--bind", "0.0.0.0"]
