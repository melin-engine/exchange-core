/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use trading_protocol::tcp::BlockingTcpListener;
use trading_server::server::ServerConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let config = ServerConfig::from_args(&args).map_err(|e| {
        eprintln!("error: {e}");
        eprintln!();
        eprintln!("usage: trading-server [OPTIONS]");
        eprintln!();
        eprintln!("  --bind=<addr>            listen address (default: 127.0.0.1:9876)");
        eprintln!("  --journal=<path>         journal file path (default: trading.journal)");
        eprintln!("  --snapshot=<path>        snapshot file path");
        eprintln!("  --cores=<j,m,r>          pipeline core IDs (default: 1,2,3)");
        eprintln!("  --readers=<n>            reader thread count (default: 2)");
        eprintln!("  --reader-cores=<start>   first reader core ID (default: 4)");
        eprintln!("  --group-commit-us=<n>    group commit delay in µs (default: 0)");
        e
    })?;
    let listener = BlockingTcpListener::bind(config.bind_addr)?;
    trading_server::server::run(listener, config)
}
