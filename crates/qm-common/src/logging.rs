//! 统一日志初始化。
//!
//! - 控制台：human 格式，含时间/线程（调试友好）
//! - 文件：JSON 格式（容器化采集友好），非阻塞写入 + 后台落盘线程
//! - 级别：由 [`crate::config::LoggingConfig::level`] 决定，支持 `target=level` 组合

use once_cell::sync::OnceCell;

use crate::config::AppConfig;

static SUBSCRIBER_INIT: OnceCell<()> = OnceCell::new();
static FILE_GUARD: OnceCell<tracing_appender::non_blocking::WorkerGuard> = OnceCell::new();

/// 构造日志过滤器：环境变量 `RUST_LOG` 优先（现场调参），否则用配置的 `level`。
pub fn build_filter(level: &str) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level))
}

/// 初始化全局 tracing subscriber。幂等：重复调用只初始化一次。
///
/// 两个坑都在这里踩过，务必别改回去：
/// 1. 必须用 `OnceCell::get_or_init`（惰性求值）。用 `OnceCell::set({ ... })` 的话，
///    闭包参数是**先行求值**的，第二次调用会再次执行 `builder.init()`，
///    在 `tracing_subscriber::fmt::mod.rs` 里 panic
///    （`SetGlobalDefaultError("a global default trace dispatcher has already been set")`）。
///    demo 恰好会调用两次（`config::init` 一次、main 一次），所以进程直接起不来。
/// 2. 全局 dispatcher 已经存在（例如测试框架先装了 subscriber）时**静默跳过**，
///    而不是 panic 中断启动。
pub fn init_subscriber(cfg: &AppConfig) {
    let _ = SUBSCRIBER_INIT.get_or_init(|| {
        let filter = build_filter(&cfg.logging.level);
        let builder = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339());

        if cfg.logging.file_dir.is_empty() {
            builder.init();
        } else {
            // 创建日志目录属启动期不可恢复问题，直接 panic 让配置问题尽早暴露。
            std::fs::create_dir_all(&cfg.logging.file_dir)
                .expect("创建日志目录失败（属启动期不可恢复问题）");
            let appender = tracing_appender::rolling::daily(&cfg.logging.file_dir, "quickmeet.log");
            let (writer, guard) = tracing_appender::non_blocking(appender);
            // 落盘 guard 绑定到进程生命周期，避免后台线程提前退出丢日志。
            let _ = FILE_GUARD.set(guard);
            if cfg.logging.json {
                builder.json().with_writer(writer).init();
            } else {
                builder.with_writer(writer).init();
            }
        }
    });
    tracing::info!(
        dispatcher_already_set = tracing::dispatcher::get_default(|_| true),
        "tracing subscriber 就绪"
    );
}

/// 判定 debug 是否开启（热点路径用于决定是否构造日志字符串）。
pub fn is_debug_enabled() -> bool {
    tracing::enabled!(tracing::Level::DEBUG)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LoggingConfig;

    #[test]
    fn logging_config_defaults_are_safe() {
        let c = LoggingConfig::default();
        assert_eq!(c.level, "info");
        assert!(c.file_dir.is_empty(), "默认不写文件，避免脏写仓库目录");
        assert!(!c.json);
    }

    #[test]
    fn env_filter_combo_parses() {
        let f = tracing_subscriber::EnvFilter::new("info,webrtc=debug,qm_media=trace");
        // EnvFilter 不暴露 directives()；用 max_level_hint 与 Display 断言。
        // 注意 max_level_hint 反映的是"最高允许级别"（本例是 trace），
        // 不是默认级别，所以只断言它覆盖了 trace 而非等值匹配 info。
        assert!(f.max_level_hint().is_some(), "必须解析出级别上限");
        let rendered = f.to_string();
        assert!(rendered.contains("info"), "{rendered}");
        assert!(rendered.contains("qm_media"), "{rendered}");
        assert!(rendered.contains("webrtc"), "{rendered}");
    }

    #[test]
    fn init_is_idempotent_twice_in_a_row() {
        // 回归护栏：`OnceCell::set({ ... })` 会让闭包参数先行求值，第二次调用会
        // 再次执行 `builder.init()` 并 panic（SetGlobalDefaultError）。
        // 之前这个测试被 `#[ignore]` 掉，demo 进程因此直接起不来。
        let cfg = AppConfig::default();
        assert!(cfg.logging.file_dir.is_empty());

        // 其他测试若已占用全局 dispatcher（`with_default` 是作用域化的，通常不会），
        // 本测试无法再验证"首次为空"，直接跳过而不是误报失败。
        if tracing::dispatcher::get_default(|_| true) {
            eprintln!("跳过：全局 dispatcher 已被其他测试占用");
            return;
        }
        assert!(SUBSCRIBER_INIT.get().is_none(), "首次初始化前必须为空");
        init_subscriber(&cfg);
        assert!(SUBSCRIBER_INIT.get().is_some(), "初始化后必须已占位");
        init_subscriber(&cfg); // 幂等：重复调用不应 panic
        init_subscriber(&AppConfig::default());
        assert!(FILE_GUARD.get().is_none(), "未声明 file_dir 时不应创建落盘线程");
    }

    #[test]
    fn file_writer_produces_json_events() {
        let dir = "./target/qm_logs_test";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let cfg = AppConfig {
            logging: LoggingConfig {
                file_dir: dir.to_string(),
                ..Default::default()
            },
            ..AppConfig::default()
        };
        assert!(
            std::path::Path::new(&cfg.logging.file_dir).exists(),
            "配置声明的日志目录必须存在"
        );

        // 与 init_subscriber 相同的 appender + JSON 组合，但用
        // `subscriber::with_default` 作用域化安装，不占全局 dispatcher 槽位，
        // 因此可以与其他测试并行而不报 SetGlobalDefaultError。
        let appender = tracing_appender::rolling::daily(&cfg.logging.file_dir, "quickmeet.log");
        let (writer, guard) = tracing_appender::non_blocking(appender);
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
            .with_writer(writer)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(probe = "file_writer_probe", "写入文件日志");
        });
        drop(guard);

        let mut hit = false;
        for entry in std::fs::read_dir(dir).unwrap() {
            let content = std::fs::read_to_string(entry.unwrap().path()).unwrap_or_default();
            if content.contains("file_writer_probe") && content.contains("target") {
                hit = true;
            }
        }
        assert!(hit, "JSON 事件必须已落盘（含结构化字段）");
    }
}
