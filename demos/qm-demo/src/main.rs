//! QuickMeet 可本地运行的 demo（验收标准 4）。
//!
//! 用法：
//!   cargo run --release                          # 输出三 codec 收发兼容验证报告
//!   cargo run --release -- --config ./config      # 使用自定义配置目录
//!   cargo run --release -- --signal               # 顺带在配置的内网地址上启动信令服务
//!
//! 默认配置（见 `config/default.toml`）：
//!   媒体端口 8080、信令端口 8081、内网 192.168.0.0/24、绑 192.168.0.10。
//! 本机若没有 192.168.0.10 网卡，用环境变量覆盖即可，不用改代码：
//!   QM_NETWORK_BIND_HOST=127.0.0.1 cargo run --release -- --signal

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use qm_common::{config, logging};

#[derive(Debug, Parser)]
#[command(name = "qm-demo", version, about = "QuickMeet Stage 1 demo：编解码收发验证 + 内网信令服务")]
struct Args {
    /// 配置目录（含 default.toml / local.json）
    #[arg(long, default_value = "./config")]
    config: String,

    /// 顺带启动信令服务（Ctrl+C 退出）
    #[arg(long)]
    signal: bool,

    /// 媒体服务监听地址（覆盖配置的 network.bind_host，本机测试用 127.0.0.1）
    #[arg(long)]
    bind: Option<String>,

    /// 每个 codec 验证的帧数
    #[arg(long, default_value_t = 10)]
    frames: u32,

    /// 把验证报告以 JSON 写到 data/ 下（本地落盘，符合数据本地化约束）
    #[arg(long)]
    json_report: bool,
}

/// 主流程。
fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    let mut cfg = config::init(&args.config)
        .with_context(|| format!("配置加载失败（目录 {}）", args.config))?;
    if let Some(host) = &args.bind {
        cfg.network.bind_host = host.clone();
    }
    logging::init_subscriber(&cfg);

    tracing::info!(
        media_port = cfg.media.port,
        signaling_port = cfg.media.signaling_port,
        cidrs = ?cfg.network.cidrs,
        bind = %cfg.network.bind_host,
        "QuickMeet demo 启动"
    );

    print_codec_report(args.frames)?;
    save_json_report(&cfg, args.frames, args.json_report)?;

    if args.signal {
        tracing::info!("启动信令服务（Ctrl+C 退出）");
        let signal_cfg = Arc::new(cfg);
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(async move {
                tokio::select! {
                    r = qm_signaling::start(signal_cfg) => r?,
                    _ = tokio::signal::ctrl_c() => { tracing::info!("收到 Ctrl+C，demo 退出"); }
                }
                Ok::<(), anyhow::Error>(())
            })?;
    }

    Ok(())
}

/// 打印三 codec 的收发兼容验证报告（验收标准 3 的可复现结果）。
fn print_codec_report(frames: u32) -> anyhow::Result<()> {
    tracing::info!(frames, "开始编解码收发兼容验证");
    let reports = qm_media::verify::run_all_frames(frames)?;
    println!("{}", qm_media::verify::render(&reports));
    let failed = reports.iter().filter(|r| !r.ok()).count();
    for r in &reports {
        tracing::info!(
            codec = %r.codec,
            pt = r.payload_type,
            clock = r.clock_rate,
            sent = r.frames_sent,
            recv = r.frames_received,
            bytes_in = r.bytes_in,
            bytes_wire = r.bytes_on_wire,
            lossless = r.lossless,
            deterministic = r.deterministic,
            integrity = r.integrity_guarded,
            "codec 验证结果"
        );
    }
    if failed != 0 {
        anyhow::bail!("{failed} 个 codec 验证未通过");
    }
    tracing::info!("全部 codec 收发兼容验证通过");
    Ok(())
}

/// 需要时把验证报告落盘（默认 `./data/codec-report.json`）。
fn save_json_report(
    cfg: &qm_common::AppConfig,
    frames: u32,
    json_report: bool,
) -> anyhow::Result<()> {
    if !json_report {
        return Ok(());
    }
    let reports = qm_media::verify::run_all_frames(frames)?;
    let report = serde_json::json!({
        "app": qm_common::NAME,
        "version": qm_common::VERSION,
        "msrv": qm_common::MSRV,
        "frames_per_codec": frames,
        "media_port": cfg.media.port,
        "cidrs": cfg.network.cidrs,
        "generated_at": chrono_utc_now(),
        "reports": reports,
    });
    let dir = &cfg.storage.data_dir;
    std::fs::create_dir_all(dir)
        .with_context(|| format!("创建数据目录失败：{dir}"))?;
    let path = format!("{dir}/codec-report.json");
    qm_common::storage::atomic_write_json(&path, &report)?;
    tracing::info!(%path, "验证报告已落盘（本地存储）");
    Ok(())
}

/// UTC 时间戳（ISO 8601，便于把报告和时间对齐）。
fn chrono_utc_now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// 入口：把内部统一错误转成退出码。
fn main() {
    match run() {
        Ok(()) => {}
        Err(e) => {
            let msg = format!("demo 失败：{e:#}");
            tracing::error!(error = %msg, "退出");
            eprintln!("{msg}");
            std::process::exit(1);
        }
    }
}


#[cfg(test)]
mod tests {
    use clap::Parser;

    #[test]
    fn defaults_are_intranet_safe() {
        let cfg = qm_common::AppConfig::default();
        assert_eq!(cfg.media.port, 8080, "媒体端口必须默认 8080");
        assert_eq!(cfg.media.signaling_port, 8081);
        assert!(
            cfg.network.cidrs.iter().any(|c| c == "192.168.0.0/24"),
            "默认必须包含内网 192.168.0.0/24"
        );
    }

    #[test]
    fn cli_parses() {
        let args = crate::Args::parse_from(["qm-demo", "--frames", "5", "--bind", "127.0.0.1"]);
        assert_eq!(args.frames, 5);
        assert_eq!(args.bind.as_deref(), Some("127.0.0.1"));
        assert!(!args.signal);
    }

    #[test]
    fn demo_smoke_report_is_all_pass() {
        let reports = qm_media::verify::run_all_frames(3).unwrap();
        assert_eq!(reports.len(), 3);
        assert!(reports.iter().all(|r| r.ok()), "demo 默认报告必须全部 PASS");
    }
}
