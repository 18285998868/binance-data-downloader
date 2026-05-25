//! CLI 入口。
//!
//! 职责：解析 `-c <config.toml>` 参数 → 加载配置 → 初始化日志 → 委托 `data_downloader::run()`。
//! 配置加载失败时直接退出（不进入 async runtime），避免在已损坏状态下启动下载管线。

use clap::Parser;

/// CLI 参数结构体。
#[derive(Parser)]
#[command(name = "data-downloader", version)]
struct Cli {
    /// 配置文件路径。默认指向 workspace-root 下的 `config/data-downloader/config.example.toml`。
    #[arg(short = 'c', long, default_value = "config/data-downloader/config.example.toml")]
    config_file: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), data_downloader::AppError> {
    let cli = Cli::parse();

    // 加载 TOML 配置。失败时直接退出，不走 async 路径。
    let config = data_downloader::config::load_config(Some(&cli.config_file)).unwrap_or_else(|e| {
        eprintln!("config error: {e}");
        std::process::exit(1);
    });

    // 从配置中读取日志级别，初始化 env_logger。
    // 使用 Builder::new() 而非 unsafe set_var，确保多线程安全。
    let filter = config.resolved_log_level().parse::<log::LevelFilter>()
        .unwrap_or(log::LevelFilter::Debug);
    env_logger::Builder::new()
        .filter_level(filter)
        .init();

    let symbols_count = match &config.symbols {
        Some(s) => format!("{}", s.len()),
        None => "auto".into(),
    };
    log::info!(
        "config loaded: symbols={} timeframes={} market_types={:?} data_dir={}",
        symbols_count,
        config.resolved_timeframes().len(),
        config.market_types(),
        config.resolved_data_dir().display(),
    );
    log::info!("starting binance data downloader");
    data_downloader::run(&config).await?;
    log::info!("all done");

    Ok(())
}
