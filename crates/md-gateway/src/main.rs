//! FIX 4.4 market data gateway for Melin.
//!
//! Connects to the melin event publisher for order book state, then
//! serves FIX 4.4 MarketDataRequest (V) → MarketDataSnapshotFullRefresh (W)
//! and MarketDataRequestReject (Y) to connected clients.
//!
//! Usage:
//!   melin-md-gateway --config md-gateway.toml [--core N]

mod config;
pub mod translate;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut config_path: Option<String> = None;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                config_path = Some(args.get(i).cloned().unwrap_or_default());
            }
            _ => {
                eprintln!("usage: melin-md-gateway --config <path>");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let config_path = config_path.unwrap_or_else(|| {
        eprintln!("usage: melin-md-gateway --config <path>");
        std::process::exit(1);
    });

    let config_str = std::fs::read_to_string(&config_path).unwrap_or_else(|e| {
        eprintln!("failed to read config {config_path}: {e}");
        std::process::exit(1);
    });

    let config: config::GatewayConfig = toml::from_str(&config_str).unwrap_or_else(|e| {
        eprintln!("failed to parse config: {e}");
        std::process::exit(1);
    });

    tracing::info!(
        listen = %config.listen,
        event_publisher = %config.event_publisher,
        symbols = config.symbols.len(),
        "melin-md-gateway starting"
    );

    // TODO: Phase 6 — io_uring event loop, FIX session handling,
    // MarketDataCore thread, and FIX V/W/Y dispatch.
    tracing::warn!("md-gateway event loop not yet implemented");
}
