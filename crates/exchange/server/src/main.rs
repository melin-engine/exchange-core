/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc tuning, applied at allocator init via the well-known
/// `malloc_conf` symbol. Set for tail-latency stability:
///
/// - `background_thread:true` — spawn a dedicated thread to do page
///   purging asynchronously instead of synchronously on the allocating
///   thread. Default jemalloc does the purge work on whatever thread
///   happens to free memory, which on the matching/journal hot path
///   shows up as occasional multi-millisecond stalls in `process_event`.
/// - `dirty_decay_ms:53000` / `muzzy_decay_ms:57000` — hold dirty/muzzy
///   pages for ~53/57 s (vs the 10 s default) before reclaiming. Trades
///   marginally higher steady-state RSS for fewer purge events; with
///   the background thread this also bounds how often that thread runs.
///   Values are deliberately odd so purge-induced latency spikes are
///   immediately attributable to jemalloc rather than blending into
///   60 s monitoring/heartbeat boundaries.
///
/// The trailing NUL is required: jemalloc reads `malloc_conf` as a C
/// string. `non_upper_case_globals` is the documented spelling — the
/// symbol name has to match exactly.
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] =
    b"background_thread:true,dirty_decay_ms:53000,muzzy_decay_ms:57000\0";

use clap::Parser;
use melin_ec_server::event_publisher;
use melin_ec_server::request_decoder::RequestDecoder;
use melin_ec_server::response_encoder::ResponseEncoder;
use melin_ec_server::{ServerApp, StartupConfig};
use melin_server_runtime::server::{self, ServerConfig};

/// The node's command line: the sequencer runtime's flags, plus the
/// trading-specific ones the runtime does not carry.
#[derive(Parser)]
#[command(name = "melin-ec-server", about = "Melin Exchange Core server")]
struct Cli {
    #[command(flatten)]
    server: ServerConfig,
    #[command(flatten)]
    startup: StartupConfig,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    let Cli {
        server: config,
        startup,
    } = Cli::parse();

    server::run::<ServerApp>(
        config,
        startup.startup_events(),
        startup.sizing(),
        RequestDecoder,
        ResponseEncoder,
        Some(event_publisher::run),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// The runtime's flags and the node's share one namespace, and the
    /// runtime's change on the sequencer's schedule. clap reports a clash
    /// only when it builds the command, so build it here rather than on a
    /// node's first start.
    #[test]
    fn command_line_is_well_formed() {
        Cli::command().debug_assert();
    }

    #[test]
    fn node_and_runtime_flags_parse_together() {
        let cli = Cli::try_parse_from([
            "melin-ec-server",
            "--journal",
            "node.journal",
            "--max-orders-per-account",
            "2",
        ])
        .unwrap();
        assert_eq!(cli.server.journal, std::path::Path::new("node.journal"));
        assert_eq!(cli.startup.max_orders_per_account, 2);
    }
}
