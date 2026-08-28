//! Trading engine benchmark suite with three modes:
//!
//! **`--mode=roundtrip`** (default): Full end-to-end benchmark. By default, boots
//! the server in-process and connects via TCP loopback. With `--addr=<ip:port>`,
//! connects to a remote engine instead (LAN benchmark mode). With `--uds`,
//! uses Unix domain sockets. Measures client-perceived latency including
//! transport, queuing, journaling, and matching.
//!
//! **`--mode=pipeline`**: Server pipeline without network transport. Publishes
//! events directly to the disruptor ring buffer and consumes responses from the
//! output SPSC queue. Isolates journal + matching stage latency from TCP/UDS
//! overhead.
//!
//! **`--mode=engine`**: Matching engine only. Calls `Exchange::execute()` directly
//! in a tight loop — no disruptor, no journal, no I/O. Measures pure matching
//! engine throughput and latency.
//!
//! All modes use the realistic order flow generator: a mix of limit orders
//! and cancels with power-law price/size distributions, multiple accounts,
//! and resting book depth. Orders are generated on-the-fly inside the hot
//! loop so memory stays bounded regardless of run length.
//!
//! Run length is wall-clock-driven. Each phase is a duration:
//!
//! * `--warmup-duration` (default 5s) — primes caches; samples discarded.
//! * `--duration`        (default 60s) — measured into the histogram.
//! * `--cooldown-duration` (default 5s) — drains the journal/network tail;
//!   samples discarded.
//!
//! Completions are classified by *receive time* against shared phase
//! deadlines, so all bench threads agree on which phase a sample belongs
//! to without further coordination.
//!
//! Usage:
//!     cargo run --release --bin melin-bench -- \
//!         [--mode=roundtrip|pipeline|engine] [--uds] [--addr=<ip:port>] \
//!         [--health-addr=<ip:port>] [--clients=N] [--window=N] \
//!         [--bench-threads=N] [--warmup-duration=5s] [--duration=60s] \
//!         [--cooldown-duration=5s] [--group-commit-us=N]
//!
//! Default: roundtrip mode, TCP transport, 60 s measured.

// Under `--features dpdk`, the entire TCP-path code in this file is
// unreachable from the dispatch in `main`. Suppress the resulting
// dead-code warnings rather than cfg-gating every TCP helper
// individually.
#![cfg_attr(feature = "dpdk", allow(dead_code))]

mod generator;
mod workload;

#[cfg(feature = "dpdk")]
mod dpdk;

/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(not(feature = "dpdk"))]
use std::io::Write;
use std::num::NonZeroU64;
#[cfg(not(feature = "dpdk"))]
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

// Transport-agnostic harness. Nothing re-exported here knows what an
// order is; the exchange-shaped half of the bench lives in this crate.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
use melin_bench_harness::clock::{calibrate_tsc, calibrate_tsc_clock, rdtscp, tsc_to_ns};
use melin_bench_harness::pacing::{PaceClock, PaceStats};
use melin_bench_harness::phases::{
    BenchPhases, DEFAULT_BENCH_THREADS, DEFAULT_CLIENTS, DEFAULT_COOLDOWN, DEFAULT_DURATION,
    DEFAULT_WARMUP, DEFAULT_WINDOW, parse_duration,
};
use melin_bench_harness::report::{PacingReport, RunReport, Unit, print_results};
use melin_bench_harness::series::{TimeSeries, maybe_sample};
// The kernel-transport path hands its connections to the harness's
// io_uring loop; the DPDK path runs its own loop over smoltcp sockets.
#[cfg(not(feature = "dpdk"))]
use melin_bench_harness::transport::{connect_tcp, connect_uds};
#[cfg(not(feature = "dpdk"))]
use melin_bench_harness::uring::{self, Connection, LoopConfig};
use melin_bench_harness::{health, keys, stats};
#[cfg(not(feature = "dpdk"))]
use workload::ExchangeWorkload;
use workload::{OutcomeReport, enforce_rejection_threshold};

/// What this benchmark counts, for the harness's console labels.
pub(crate) const ORDER: Unit = Unit {
    singular: "order",
    plural: "orders",
    heading: "Per-Order",
};

#[cfg(not(feature = "dpdk"))]
use melin_protocol::codec;
// Only `auth_handshake` names response variants directly now; the DPDK
// path runs its own handshake.
#[cfg(not(feature = "dpdk"))]
use melin_protocol::message::ResponseKind;
use melin_server::exchange_app::ServerApp;
#[cfg(not(feature = "dpdk"))]
use melin_server_runtime::server::ServerConfig;
use melin_types::types::*;
#[cfg(not(feature = "dpdk"))]
use melin_wire_protocol::transport::BlockingTransportListener;

/// Maximum frame payload size (matches protocol).
#[cfg(not(feature = "dpdk"))]
const MAX_FRAME_SIZE: usize = 1024;

/// Benchmark CLI arguments.
#[derive(clap::Parser)]
#[command(name = "melin-bench", about = "Matching engine benchmark suite")]
struct BenchArgs {
    /// Benchmark mode: roundtrip (full server), pipeline (no network), engine (matching only).
    #[arg(long, default_value = "roundtrip")]
    mode: String,
    /// Use Unix domain sockets instead of TCP (roundtrip mode only).
    #[arg(long)]
    uds: bool,
    /// Connect to a remote engine instead of spawning an embedded server (roundtrip mode only).
    #[arg(long)]
    addr: Option<std::net::SocketAddr>,
    /// Length of the measured phase. Accepts humantime values
    /// (e.g. `30s`, `2m`, `500ms`).
    #[arg(long, default_value_t = humantime::Duration::from(DEFAULT_DURATION), value_parser = parse_duration)]
    duration: humantime::Duration,
    /// Orders in flight per client (pipelining depth).
    #[arg(long, default_value_t = DEFAULT_WINDOW)]
    window: usize,
    /// Number of concurrent client connections.
    #[arg(long, default_value_t = DEFAULT_CLIENTS)]
    clients: usize,
    /// Number of bench client threads. Each thread gets its own io_uring ring.
    #[arg(long, default_value_t = DEFAULT_BENCH_THREADS)]
    bench_threads: usize,
    /// Group commit coalescing delay in microseconds.
    #[arg(long, default_value_t = 0)]
    group_commit_us: u64,
    /// Target send rate in orders/sec (open-loop pacing). `0` (default)
    /// disables pacing and falls back to closed-loop window-filling.
    /// When set, each client thread schedules sends at fixed intervals
    /// and pushes the *scheduled* timestamp into the latency histogram —
    /// the standard fix for coordinated omission. `--window` still acts
    /// as a hard inflight cap; if the server stalls and the cap engages
    /// the bench reports `late_sends` rather than silently absorbing the
    /// back-pressure.
    #[arg(long, default_value_t = 0)]
    target_rate: u64,
    /// Warmup duration before measurement starts. Lets caches, branch
    /// predictors, and allocator arenas settle. Accepts humantime values.
    #[arg(long, default_value_t = humantime::Duration::from(DEFAULT_WARMUP), value_parser = parse_duration)]
    warmup_duration: humantime::Duration,
    /// Cooldown duration after measurement ends. The bench's final batch
    /// flushes a small number of events whose `fdatasync` cost isn't
    /// amortised across a full batch, inflating the run-max with a
    /// drain-tail artefact that doesn't reflect steady-state behaviour.
    /// Samples recorded during cooldown are discarded.
    #[arg(long, default_value_t = humantime::Duration::from(DEFAULT_COOLDOWN), value_parser = parse_duration)]
    cooldown_duration: humantime::Duration,
    /// Path for the journal file. Defaults to a temporary directory.
    /// Use this to place the journal on a dedicated disk for benchmarking.
    #[arg(long)]
    journal: Option<std::path::PathBuf>,
    /// Pipeline-mode core assignment, five comma-separated IDs in the
    /// order `journal,matching,publisher,journal-disk,drain`. 0 leaves
    /// an entry unpinned. Every one of these threads busy-spins, so two
    /// on one core starve each other — the values are checked for
    /// duplicates before anything is spawned. Keep `journal` and
    /// `journal-disk` on the same CCD: they exchange a cache line on
    /// every batch. Ignored outside `--mode pipeline`.
    #[arg(long, value_delimiter = ',', default_value = "1,2,3,4,5")]
    pipeline_cores: Vec<usize>,
    /// Number of trading accounts.
    #[arg(long, default_value_t = 10_000)]
    accounts: u32,
    /// Number of instruments.
    #[arg(long, default_value_t = 100)]
    instruments: u32,
    /// Write results to a JSON file. Useful for building saturation curves
    /// from multiple runs with different load levels.
    #[arg(long)]
    json: Option<std::path::PathBuf>,
    /// Path to a 32-byte raw Ed25519 private key file for authentication
    /// (required for remote mode with --addr, auto-generated for embedded).
    #[arg(long)]
    key: Option<std::path::PathBuf>,

    // --- DPDK options (only with --features dpdk) ---
    /// DPDK EAL arguments (space-separated).
    #[arg(long, default_value = "", allow_hyphen_values = true)]
    dpdk_eal_args: String,
    /// DPDK port IDs, comma-separated (default: "0"). For LACP bonds use "0,1".
    #[arg(long, default_value = "0", value_delimiter = ',')]
    dpdk_ports: Vec<u16>,
    /// Local IPv4 address for the DPDK bench interface.
    #[arg(long, default_value = "10.0.0.2")]
    dpdk_ip: String,
    /// IPv4 prefix length for the DPDK bench interface.
    #[arg(long, default_value_t = 24)]
    dpdk_prefix_len: u8,
    /// IPv4 gateway for the DPDK bench interface.
    #[arg(long)]
    dpdk_gateway: Option<String>,
    /// Peer IPv4 used for the bifurcated `rte_flow` steering rule. When
    /// set, the DPDK port opens in isolated mode and only IPv4 packets
    /// sourced from this address are delivered into DPDK queue 0 —
    /// everything else stays with the kernel netdev. Required for L3
    /// setups that share the public NIC with the kernel (SSH, etc.).
    #[arg(long)]
    dpdk_peer_ip: Option<String>,
    /// Gateway MAC (aa:bb:cc:dd:ee:ff) seeded into smoltcp for the
    /// `--dpdk-gateway` IP. Used in L3 bifurcated mode where the ARP
    /// reply for the gateway would not match the steering rule and
    /// would be eaten by the kernel. Source it from `ip neigh` on the
    /// host.
    #[arg(long)]
    dpdk_gateway_mac: Option<String>,
    /// Server MAC (aa:bb:cc:dd:ee:ff) seeded into smoltcp for `--addr`,
    /// so the first frame is addressed correctly without an ARP round
    /// trip. Required in mlx5 bifurcated mode: there the DPDK port
    /// shares the kernel netdev's real hardware MAC, not the SR-IOV
    /// `02:00:<ip>` address the fallback derives. Read it from
    /// `/sys/class/net/<iface>/address` on the server.
    #[arg(long)]
    dpdk_peer_mac: Option<String>,
    /// MTU for the DPDK interface. Use 9000 for jumbo frames. Must match server.
    #[arg(long, default_value_t = 1500)]
    dpdk_mtu: usize,
    /// VLAN ID for hardware strip/insert. Required for dedicated NIC mode.
    #[arg(long)]
    dpdk_vlan: Option<u16>,
    /// CPU core for the DPDK bench poll thread.
    #[arg(long, default_value_t = 7)]
    dpdk_core: usize,
    /// First CPU core for bench thread pinning. Thread i is pinned to core
    /// bench_cores + i. When omitted, bench threads are not pinned (OS
    /// scheduler decides). For local benchmarks against the embedded
    /// server use 12 — it has to clear every core in the server's
    /// `--cores` list, and a bench thread sharing a core with a server
    /// thread is starved by it (both are SCHED_FIFO, so they do not
    /// timeshare). For remote benchmarks on a dedicated machine, use 1 with
    /// isolcpus for tighter measurements. In engine mode this pins the
    /// (single) bench thread to `bench_cores` and sets SCHED_FIFO (when
    /// run as root) to bypass CFS load-balancer scans.
    #[arg(long)]
    bench_cores: Option<usize>,
    /// Health endpoint address to poll for server metrics during the run
    /// (roundtrip mode only). For embedded mode, auto-detected from server
    /// config. For remote mode (`--addr`), must be provided explicitly.
    #[arg(long)]
    health_addr: Option<std::net::SocketAddr>,
    /// Maximum events per journal fsync batch (pipeline mode only). Smaller
    /// values reduce tail latency, larger values improve throughput with
    /// real fsync. Default 4096. Try 256 for low-latency no-persist runs.
    #[arg(long, default_value_t = 4096)]
    max_journal_batch: usize,
    /// Fail the run (exit code 2) if more than this percent of acknowledged
    /// requests were rejected. Default 50.0% — the realistic-flow
    /// generator naturally produces a few percent of rejections (FOK
    /// can't fill, market orders on cold books, cancels of consumed
    /// orders), so the threshold is set to catch catastrophic misconfig
    /// ("most orders rejected") rather than noise. Set to 100.0 to
    /// disable; lower it for production-flow runs where rejections
    /// should be near-zero.
    #[arg(long, default_value_t = 50.0)]
    max_reject_pct: f64,
    /// Utility mode: print `authorized_keys` lines for `--clients`
    /// derived child keys and exit. Used by the LAN bench script to
    /// populate the server's authorized_keys file with the same per-
    /// client pubkeys the bench will authenticate as. Requires `--key`.
    /// No bench runs in this mode.
    #[arg(long)]
    print_authorized_keys: bool,
}

fn main() {
    // Initialize tracing so pipeline-stats and latency-trace output is visible.
    tracing_subscriber::fmt()
        .with_target(true)
        .with_thread_names(true)
        .init();

    let args = <BenchArgs as clap::Parser>::parse();

    // Utility mode — print authorized_keys lines for `--clients`
    // derived child keys and exit. Used by the LAN bench script.
    if args.print_authorized_keys {
        let key_path = args.key.as_deref().unwrap_or_else(|| {
            eprintln!("error: --key is required with --print-authorized-keys");
            std::process::exit(1);
        });
        let master = load_signing_key(key_path);
        for i in 0..args.clients {
            let child = keys::derive_client_key(&master, i as u32);
            println!(
                "{}",
                keys::authorized_keys_line(&child, &format!("bench-{i}"))
            );
        }
        return;
    }

    let json_path = args.json.as_deref();
    let phases = BenchPhases {
        warmup: args.warmup_duration.into(),
        measured: args.duration.into(),
        cooldown: args.cooldown_duration.into(),
    };

    // --target-rate requires a non-zero --window: with window=0 the bench
    // cannot keep any inflight sends, so paced sends would never reach the
    // server. Fail loud rather than silently producing a 0-throughput run.
    if args.target_rate > 0 && args.window == 0 {
        eprintln!("error: --target-rate requires --window > 0 (current: 0)");
        std::process::exit(1);
    }

    match args.mode.as_str() {
        "engine" => {
            run_engine_bench(
                phases,
                args.accounts,
                args.instruments,
                json_path,
                args.target_rate,
                args.max_reject_pct,
                args.bench_cores,
            );
        }
        "pipeline" => {
            let cores = match resolve_pipeline_cores(&args.pipeline_cores) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(2);
                }
            };
            run_pipeline_bench(
                phases,
                args.window,
                args.group_commit_us,
                args.journal,
                json_path,
                args.max_journal_batch,
                args.target_rate,
                args.max_reject_pct,
                cores,
            );
        }
        "roundtrip" => {
            #[cfg(feature = "dpdk")]
            {
                let addr = args.addr.unwrap_or_else(|| {
                    eprintln!("error: --addr is required for DPDK mode (no embedded server)");
                    std::process::exit(1);
                });
                let key_path = args.key.as_deref().unwrap_or_else(|| {
                    eprintln!("error: --key is required for DPDK mode");
                    std::process::exit(1);
                });
                let key = load_signing_key(key_path);

                dpdk::run_dpdk_roundtrip(
                    args.max_reject_pct,
                    dpdk::DpdkBenchConfig {
                        eal_args: args
                            .dpdk_eal_args
                            .split_whitespace()
                            .map(String::from)
                            .collect(),
                        port_ids: args.dpdk_ports.clone(),
                        local_ip: args.dpdk_ip.parse().expect("invalid --dpdk-ip"),
                        prefix_len: args.dpdk_prefix_len,
                        gateway: args
                            .dpdk_gateway
                            .as_deref()
                            .map(|s| s.parse().expect("invalid --dpdk-gateway")),
                        server_addr: addr,
                        mtu: args.dpdk_mtu,
                        vlan_id: args.dpdk_vlan,
                        peer_ip: args
                            .dpdk_peer_ip
                            .as_deref()
                            .map(|s| s.parse().expect("invalid --dpdk-peer-ip")),
                        gateway_mac: args.dpdk_gateway_mac.as_deref().map(melin_dpdk::parse_mac),
                        peer_mac: args.dpdk_peer_mac.as_deref().map(melin_dpdk::parse_mac),
                    },
                    phases,
                    args.window,
                    args.clients,
                    json_path,
                    &key,
                    args.accounts,
                    args.instruments,
                    args.dpdk_core,
                    args.health_addr,
                    args.target_rate,
                );
            }

            #[cfg(not(feature = "dpdk"))]
            {
                run_roundtrip_bench(
                    args.uds,
                    phases,
                    args.window,
                    args.clients,
                    args.bench_threads,
                    args.group_commit_us,
                    args.addr,
                    args.journal,
                    args.accounts,
                    args.instruments,
                    json_path,
                    args.key.as_deref(),
                    args.bench_cores,
                    args.health_addr,
                    args.target_rate,
                    args.max_reject_pct,
                );
            }
        }
        other => {
            eprintln!("unknown mode: {other} (expected: engine, pipeline, roundtrip)");
            std::process::exit(1);
        }
    }
}

// ===========================================================================
// Engine-only benchmark
// ===========================================================================

/// Engine-only benchmark with realistic order flow. Calls `Exchange::execute()`
/// and `Exchange::cancel()` directly in a tight loop — no disruptor, no journal,
/// no I/O. Uses the generator to produce a mix of limit orders and cancels with
/// power-law price/size distributions, multiple accounts, and resting book depth.
/// Orders are generated on-the-fly inside the loop; `next_event()` is invoked
/// *before* the per-order `rdtscp()` so RNG cost stays outside the measured
/// window.
fn run_engine_bench(
    phases: BenchPhases,
    num_accounts: u32,
    num_instruments: u32,
    json_path: Option<&std::path::Path>,
    target_rate: u64,
    max_reject_pct: f64,
    bench_cores: Option<usize>,
) {
    use generator::{GeneratedEvent, GeneratorConfig, OrderFlowGenerator};

    // Pin + RT the bench thread when --bench-cores is set. `pin_to_core`
    // both pins affinity and (on non-zero cores) sets SCHED_FIFO priority
    // 1 — bypassing the CFS periodic load-balancer scans that otherwise
    // show up as a 1Hz, ~18µs cluster at p99.99999 on isolated cores.
    // Matches the scheduler class the server's pipeline threads
    // (matching/journal/response/etc.) already run under in production
    // when started as root. Failure to set RT (no root, no CAP_SYS_NICE)
    // is logged but non-fatal.
    if let Some(core) = bench_cores {
        match melin_app::affinity::pin_to_core(core) {
            Ok(_) => eprintln!("bench thread pinned to core {core} (SCHED_FIFO if root)"),
            Err(e) => eprintln!("warning: pin_to_core({core}) failed: {e}"),
        }
    }

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    let ticks_per_ns = calibrate_tsc();
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    eprintln!(
        "TSC calibration: {:.3} GHz ({:.2} ticks/ns)",
        ticks_per_ns, ticks_per_ns
    );

    let config = GeneratorConfig {
        num_accounts,
        num_instruments,
        ..Default::default()
    };

    let mut exchange = melin_exchange_core::exchange::Exchange::with_capacity();

    // Register instruments.
    for i in 1..=num_instruments {
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(i),
            base: CurrencyId(i * 2 - 1),
            quote: CurrencyId(i * 2),
        });
    }

    // Provision all accounts with generous balances in all currencies.
    for acct in 1..=num_accounts {
        exchange.provision_account(AccountId(acct), u64::MAX / 4);
    }

    exchange.prefault();

    let mut flow = OrderFlowGenerator::new(config);

    let mut reports = Vec::with_capacity(256);
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");

    let phase_start = Instant::now();
    let deadlines = phases.deadlines(phase_start);

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    let pace_stats = PaceStats::default();
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        if target_rate > 0 {
            eprintln!(
                "warning: --target-rate ignored on this architecture (requires TSC; x86_64 or aarch64)"
            );
        }
    }

    // Warmup — drive the engine at full speed but discard timings. Polling
    // `Instant::now()` every iteration is fine because the warmup body is
    // already many hundreds of ns of work; the clock read is negligible
    // and stops the loop precisely without burning extra cycles.
    while Instant::now() < deadlines.warmup_end {
        reports.clear();
        let event = flow.next_event();
        match event {
            GeneratedEvent::Submit { symbol, order } => {
                exchange.execute(symbol, order, &mut reports);
            }
            GeneratedEvent::Cancel {
                symbol,
                account,
                order_id,
            } => {
                exchange.cancel(symbol, account, order_id, &mut reports);
            }
            GeneratedEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                exchange.cancel_replace(
                    symbol,
                    account,
                    order_id,
                    new_price,
                    new_quantity,
                    &mut reports,
                );
            }
        }
    }

    // Measured run.
    let mut interval_hist =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("interval histogram");
    let mut interval_count: usize = 0;
    // Pre-allocate generously: at 10M ord/s × SAMPLE_INTERVAL=1000 across
    // typical bench durations (≤ 10 min) we push ≤ 600k entries; sizing
    // for that up-front avoids the doubling-reallocate spikes that show
    // up as ~100µs outliers in the deep tail at the 32k/64k/128k/256k
    // capacity boundaries.
    let mut series: TimeSeries = Vec::with_capacity(600_000);

    let mut submits: u64 = 0;
    let mut cancels: u64 = 0;
    let mut amends: u64 = 0;

    // Track the N slowest orders for post-run diagnostics.
    // Min-heap by latency: the smallest is at the top so we can
    // efficiently evict it when a slower order arrives. Wrapped in a
    // local struct because `GeneratedEvent` isn't Ord — heap ordering
    // is by `latency_ns` only.
    const SLOWEST_N: usize = 10;
    #[derive(Clone, Copy)]
    struct SlowEntry {
        latency_ns: u64,
        event: GeneratedEvent,
        num_reports: usize,
        offset_us: u64,
    }
    impl PartialEq for SlowEntry {
        fn eq(&self, o: &Self) -> bool {
            self.latency_ns == o.latency_ns
        }
    }
    impl Eq for SlowEntry {}
    impl PartialOrd for SlowEntry {
        fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for SlowEntry {
        fn cmp(&self, o: &Self) -> std::cmp::Ordering {
            self.latency_ns.cmp(&o.latency_ns)
        }
    }
    let mut slowest: std::collections::BinaryHeap<std::cmp::Reverse<SlowEntry>> =
        std::collections::BinaryHeap::with_capacity(SLOWEST_N + 1);

    // Outcome counters span both measured and cooldown loops below so a
    // misconfiguration where every order is rejected fails the run loud
    // — see [`OutcomeReport`] doc.
    let mut outcomes = OutcomeReport::default();

    // Measured phase: record latencies until `measured_end` passes. We
    // poll `Instant::now()` only once per ~DEADLINE_POLL_INTERVAL
    // iterations because every per-order `Instant::now()` (~15-25 ns
    // vDSO) would inflate the engine measurement that we're trying to
    // capture in the hundreds-of-ns range. The slop is at most
    // `interval / throughput`; at 3 M ops/s × 1024 iters that's ~340 µs
    // of samples that could land just past `measured_end` and still be
    // recorded into the histogram. Negligible at any practical run
    // length, and `wall` is clamped to `phases.measured` below so
    // throughput math stays exact.
    let start = Instant::now();

    // Open-loop pacer for engine mode. Built here — *after* warmup — so
    // its TSC anchor lines up with the measured-phase start. Building it
    // before warmup would leave the schedule stale by `warmup_duration`
    // by the time the measured loop began, blasting through every
    // already-due slot and recording huge spurious late counts.
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    let mut pacer = if target_rate > 0 {
        Some(PaceClock::new(target_rate, 1, ticks_per_ns, rdtscp(), 0))
    } else {
        None
    };

    let mut iter_since_check: u32 = 0;
    const DEADLINE_POLL_INTERVAL: u32 = 1024;
    let mut measured_orders: u64 = 0;
    loop {
        if iter_since_check >= DEADLINE_POLL_INTERVAL {
            if Instant::now() >= deadlines.measured_end {
                break;
            }
            iter_since_check = 0;
        }
        iter_since_check += 1;
        reports.clear();
        let event = flow.next_event();

        // With pacing, spin until the next scheduled tick, then measure
        // from that tick (not the actual call time) so any "behind
        // schedule" engine slowness shows up as queueing latency rather
        // than being absorbed silently. Without pacing, the loop runs
        // hot and `t0` is just the per-call start tick.
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        let t0 = if let Some(p) = pacer.as_mut() {
            let scheduled = p.advance();
            while rdtscp() < scheduled {
                std::hint::spin_loop();
            }
            let now = rdtscp();
            pace_stats.record_send(now, scheduled, ticks_per_ns);
            scheduled
        } else {
            rdtscp()
        };
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let t0 = Instant::now();

        match event {
            GeneratedEvent::Submit { symbol, order } => {
                exchange.execute(symbol, order, &mut reports);
                submits += 1;
            }
            GeneratedEvent::Cancel {
                symbol,
                account,
                order_id,
            } => {
                exchange.cancel(symbol, account, order_id, &mut reports);
                cancels += 1;
            }
            GeneratedEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                exchange.cancel_replace(
                    symbol,
                    account,
                    order_id,
                    new_price,
                    new_quantity,
                    &mut reports,
                );
                amends += 1;
            }
        }

        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        let elapsed_ns = tsc_to_ns(rdtscp() - t0, ticks_per_ns);
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        let elapsed_ns = t0.elapsed().as_nanos() as u64;

        histogram.record(elapsed_ns).expect("record");
        interval_hist.record(elapsed_ns).expect("record interval");
        interval_count += 1;
        measured_orders += 1;
        maybe_sample(&mut interval_hist, &mut interval_count, &mut series, start);

        // Track top-N slowest using a min-heap capped at SLOWEST_N.
        // Only compute wall-clock offset when actually inserting (rare path).
        if slowest.len() < SLOWEST_N {
            let offset_us = start.elapsed().as_micros() as u64;
            slowest.push(std::cmp::Reverse(SlowEntry {
                latency_ns: elapsed_ns,
                event,
                num_reports: reports.len(),
                offset_us,
            }));
        } else if let Some(&std::cmp::Reverse(SlowEntry {
            latency_ns: min_ns, ..
        })) = slowest.peek()
            && elapsed_ns > min_ns
        {
            let offset_us = start.elapsed().as_micros() as u64;
            slowest.pop();
            slowest.push(std::cmp::Reverse(SlowEntry {
                latency_ns: elapsed_ns,
                event,
                num_reports: reports.len(),
                offset_us,
            }));
        }

        // Outcome tally runs *after* `elapsed_ns` was computed above, so
        // walking the reports vec is not billed to the engine-call
        // measurement. One BatchEnd per input event mirrors the
        // network-bench accounting (one BatchEnd per request).
        outcomes.batch_ends += 1;
        for r in reports.iter() {
            outcomes.record_execution_report(r);
        }
    }
    // Clamp to `phases.measured` so the reported throughput divisor
    // matches the configured measured-phase length even when the
    // deadline-poll slop overruns by up to `DEADLINE_POLL_INTERVAL`
    // iterations. Mirrors the cap in pipeline/roundtrip/DPDK paths.
    let wall = start.elapsed().min(phases.measured);

    // Cooldown — keep driving the engine to absorb any drain-tail
    // artefacts (none here in engine mode, but symmetric with the other
    // bench paths makes the phase model uniform). Samples are not
    // recorded; the histogram is sealed at this point.
    while Instant::now() < deadlines.cooldown_end {
        reports.clear();
        let event = flow.next_event();
        match event {
            GeneratedEvent::Submit { symbol, order } => {
                exchange.execute(symbol, order, &mut reports);
            }
            GeneratedEvent::Cancel {
                symbol,
                account,
                order_id,
            } => {
                exchange.cancel(symbol, account, order_id, &mut reports);
            }
            GeneratedEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                exchange.cancel_replace(
                    symbol,
                    account,
                    order_id,
                    new_price,
                    new_quantity,
                    &mut reports,
                );
            }
        }
        // Tally cooldown outcomes too — `OutcomeReport` covers the whole
        // run, mirroring the network bench's cross-phase accounting.
        outcomes.batch_ends += 1;
        for r in reports.iter() {
            outcomes.record_execution_report(r);
        }
    }

    let total_events = submits + cancels + amends;
    let cancel_pct = if total_events > 0 {
        cancels as f64 / total_events as f64 * 100.0
    } else {
        0.0
    };
    let amend_pct = if total_events > 0 {
        amends as f64 / total_events as f64 * 100.0
    } else {
        0.0
    };

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    let pacing_report = if target_rate > 0 {
        let max_delay_ns = tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        );
        Some(PacingReport {
            target_rate,
            scheduled: pace_stats.scheduled.load(Ordering::Relaxed),
            late_sends: pace_stats.late_sends.load(Ordering::Relaxed),
            max_send_delay_us: max_delay_ns as f64 / 1_000.0,
        })
    } else {
        None
    };
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let pacing_report: Option<PacingReport> = None;

    let mut extra_lines = vec![
        format!("  Accounts: {num_accounts}, Instruments: {num_instruments}"),
        format!(
            "  Submits: {submits}, Cancels: {cancels} ({cancel_pct:.1}%), Amends: {amends} ({amend_pct:.1}%)"
        ),
    ];
    if let Some(p) = pacing_report.as_ref() {
        extra_lines.push(format!(
            "  Target rate: {} ops/s (scheduled {}, late {}, max send delay {:.1} µs)",
            p.target_rate, p.scheduled, p.late_sends, p.max_send_delay_us,
        ));
    }

    print_results(RunReport {
        label: "Realistic Order Flow",
        unit: &ORDER,
        measured_count: measured_orders as usize,
        phases,
        histogram: &histogram,
        wall,
        extra_lines: &extra_lines,
        json_path,
        series: &series,
        health: &health::HealthReport::default(),
        // Engine mode runs the matching engine in-process with no
        // server / health endpoint, so there's nothing to fetch.
        server_stages: &stats::Body::Empty,
        pacing: pacing_report.as_ref(),
        outcomes: &outcomes,
    });

    // Print the slowest orders for tail latency diagnosis.
    let mut sorted: Vec<_> = slowest.into_iter().map(|std::cmp::Reverse(e)| e).collect();
    sorted.sort_by_key(|b| std::cmp::Reverse(b.latency_ns)); // descending by latency
    println!("\n  Slowest {SLOWEST_N} Orders");
    for entry in &sorted {
        let latency_us = entry.latency_ns as f64 / 1000.0;
        let offset_ms = entry.offset_us as f64 / 1000.0;
        let event = entry.event;
        let num_reports = entry.num_reports;
        println!("    {latency_us:>7.2}µs  @{offset_ms:>7.1}ms  reports={num_reports}  {event:?}");
    }

    enforce_rejection_threshold(&outcomes, max_reject_pct);
}

// ===========================================================================
// Pipeline benchmark (disruptor + journal + matching, no network)
// ===========================================================================

/// Entries `--pipeline-cores` expects: journal, matching, publisher,
/// journal-disk, drain.
const PIPELINE_CORE_SLOTS: usize = 5;

/// Pipeline-mode core assignment, resolved from `--pipeline-cores`. A
/// field of 0 leaves that thread unpinned (OS-scheduled).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PipelineBenchCores {
    journal: usize,
    matching: usize,
    publisher: usize,
    journal_disk: usize,
    drain: usize,
}

/// Parse `--pipeline-cores` and reject any layout that would put two
/// busy-spinning threads on one core. That collision does not announce
/// itself: `pin_to_core` promotes to SCHED_FIFO on an isolated core, so
/// one thread spins and the other starves, and the run reports a
/// plausible-looking but meaningless number.
fn resolve_pipeline_cores(v: &[usize]) -> Result<PipelineBenchCores, String> {
    if v.len() != PIPELINE_CORE_SLOTS {
        return Err(format!(
            "--pipeline-cores expects {PIPELINE_CORE_SLOTS} comma-separated IDs \
             (journal,matching,publisher,journal-disk,drain), got {}",
            v.len()
        ));
    }
    let cores = PipelineBenchCores {
        journal: v[0],
        matching: v[1],
        publisher: v[2],
        journal_disk: v[3],
        drain: v[4],
    };
    // (core, owner) so a duplicate can name both sides of the clash
    // rather than just the number.
    let claimed = [
        (cores.journal, "journal"),
        (cores.matching, "matching"),
        (cores.publisher, "publisher"),
        (cores.journal_disk, "journal-disk"),
        (cores.drain, "drain"),
    ];
    for i in 0..claimed.len() {
        for j in (i + 1)..claimed.len() {
            // 0 is the unpinned sentinel, not a core — any number of
            // threads may share it.
            if claimed[i].0 != 0 && claimed[i].0 == claimed[j].0 {
                return Err(format!(
                    "core {} claimed by both {} and {} — two pinned spinners on one core \
                     starve each other",
                    claimed[i].0, claimed[i].1, claimed[j].1
                ));
            }
        }
    }
    Ok(cores)
}

/// Pipeline benchmark. Builds the full disruptor pipeline (journal stage +
/// matching stage) but bypasses TCP/UDS transport. The bench thread publishes
/// InputSlots directly to the input Producer and drains OutputSlots from the
/// SPSC consumer. Measures pipeline latency without network overhead.
#[allow(clippy::too_many_arguments)]
fn run_pipeline_bench(
    phases: BenchPhases,
    window: usize,
    group_commit_us: u64,
    journal_path: Option<std::path::PathBuf>,
    json_path: Option<&std::path::Path>,
    max_journal_batch: usize,
    target_rate: u64,
    max_reject_pct: f64,
    cores: PipelineBenchCores,
) {
    use melin_journal::BufferedWriter;

    // Set up exchange with one instrument and funded account.
    let mut app = ServerApp(melin_exchange_core::exchange::Exchange::with_capacity());
    app.add_instrument(InstrumentSpec {
        symbol: Symbol(1),
        base: CurrencyId(1),
        quote: CurrencyId(2),
    });
    app.deposit(AccountId(1), CurrencyId(1), u64::MAX / 2);
    app.deposit(AccountId(1), CurrencyId(2), u64::MAX / 2);
    app.prefault();

    let tmp_dir = tempdir();
    let effective_journal = journal_path.unwrap_or_else(|| tmp_dir.join("pipeline-bench.journal"));

    let cfg = PipelineInnerCfg {
        group_commit_us,
        max_journal_batch,
        phases,
        window,
        json_path,
        target_rate,
        max_reject_pct,
        cores,
    };

    run_pipeline_inner(
        app,
        BufferedWriter::create(&effective_journal).expect("create journal"),
        cfg,
    );

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Non-writer args for [`run_pipeline_inner`], bundled so the setup in
/// [`run_pipeline_bench`] stays separate from the measurement body.
struct PipelineInnerCfg<'a> {
    group_commit_us: u64,
    max_journal_batch: usize,
    phases: BenchPhases,
    window: usize,
    json_path: Option<&'a std::path::Path>,
    target_rate: u64,
    max_reject_pct: f64,
    cores: PipelineBenchCores,
}

use melin_trading::trading_event::TradingEvent;

/// Pipeline-mode body: builds the pipeline around `writer`, spawns the
/// journal, matching and publisher threads, and drains from the calling
/// thread. Every one of those threads plus the journal's disk thread is
/// pinned per `cfg.cores` — see `--pipeline-cores`.
fn run_pipeline_inner(
    app: ServerApp,
    writer: melin_journal::BufferedWriter<TradingEvent>,
    cfg: PipelineInnerCfg<'_>,
) {
    use melin_journal::JournalEvent;
    use melin_transport_core::pipeline::OutputPayload;
    use melin_transport_core::pipeline::{InputSlot, build_pipeline_with_replication};
    use melin_transport_core::trace::mono_trace_ns;

    let PipelineInnerCfg {
        group_commit_us,
        max_journal_batch,
        phases,
        window,
        json_path,
        target_rate,
        max_reject_pct,
        cores,
    } = cfg;

    eprintln!(
        "  Pipeline cores: journal={} matching={} publisher={} journal-disk={} drain={} \
         (0 = unpinned)",
        cores.journal, cores.matching, cores.publisher, cores.journal_disk, cores.drain,
    );

    let nz = |v: u64| NonZeroU64::new(v).expect("non-zero");

    let group_commit_delay = Duration::from_micros(group_commit_us);
    let active_conns = Arc::new(AtomicU64::new(0));
    let mut out = build_pipeline_with_replication(
        app,
        writer,
        group_commit_delay,
        active_conns,
        false, // no replication
        max_journal_batch,
        melin_journal::replication::REPLICATION_RING_CAPACITY,
        true,  // busy_spin — match production default (yield_idle=false)
        false, // event_publisher
        false, // shadow
        std::sync::Arc::new(melin_transport_core::fence::FenceState::new(0)),
    );
    let mut output_consumer = out.output_consumers.pop().expect("response consumer");

    let shutdown = Arc::new(AtomicBool::new(false));

    // Spawn journal and matching stage threads.
    let shutdown_j = Arc::clone(&shutdown);
    let mut journal_stage = out.journal_stage;
    // The journal stage spawns a disk thread that busy-spins on batches
    // and publishes the durability cursors. Unpinned it is OS-scheduled
    // like any other thread, which on an isolcpus host means it lands
    // on a housekeeping core and its jitter shows up as journal tail.
    // The stage spawns it itself, from the journal thread, so the core
    // has to be handed over before `run`.
    journal_stage.set_disk_core(cores.journal_disk);
    let journal_core = cores.journal;
    let journal_handle = std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || {
            if journal_core != 0
                && let Err(e) = melin_app::affinity::pin_to_core(journal_core)
            {
                eprintln!("warning: could not pin journal to core {journal_core}: {e}");
            }
            journal_stage.run(&shutdown_j)
        })
        .expect("spawn journal thread");

    let shutdown_m = Arc::clone(&shutdown);
    let matching_stage = out.matching_stage;
    let matching_core = cores.matching;
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            if matching_core != 0
                && let Err(e) = melin_app::affinity::pin_to_core(matching_core)
            {
                eprintln!("warning: could not pin matching to core {matching_core}: {e}");
            }
            matching_stage.run(&shutdown_m)
        })
        .expect("spawn matching thread");

    // Single shared start so both threads agree on warmup/measured/cooldown
    // deadlines. Pinned threads compute their own `Instant::now()` against
    // this clock without further coordination.
    let phase_start = Instant::now();
    let deadlines = phases.deadlines(phase_start);
    let pub_stop = Arc::new(AtomicBool::new(false));

    // Split publish and drain into separate threads so the publisher
    // keeps the disruptor fed while the drainer processes BatchEnds.
    // Without this, a single thread alternates publish→drain, starving
    // the journal stage between drain phases and halving throughput.
    //
    // Coordination: inflight counter (AtomicU64) for window gating,
    // lock-free SPSC ring for timestamps (publisher → drainer).
    // Using melin_pipeline::spsc instead of std::sync::mpsc::sync_channel
    // eliminates the mutex overhead per order (~2-5µs tail reduction).
    let inflight = Arc::new(AtomicU64::new(0));
    // TSC ticks instead of Instant::now() for the latency measurement
    // (~4 ns vs ~15-25 ns per timestamp). The clock also carries an
    // epoch pair so we derive the engine-facing `timestamp_ns` from the
    // same `rdtscp()` reading the latency histogram already uses,
    // removing the per-event `clock_gettime()` that previously
    // dominated the publisher thread's profile (~9 B cycles / 6 % of
    // its samples on a 30 s capture).
    let tsc_clock = calibrate_tsc_clock();
    let ticks_per_ns = tsc_clock.ticks_per_ns;
    // SPSC channel requires capacity >= 2; clamp so `--window=1` (useful
    // for isolating pure pipeline latency without queueing) doesn't panic.
    let ts_capacity = window.next_power_of_two().max(2);
    let (mut ts_tx, mut ts_rx) = melin_pipeline::spsc::channel::<u64>(ts_capacity);

    // Publisher thread: continuously feeds events into the disruptor.
    // `sequence: 0` — the journal stage allocates sequences in disruptor
    // cursor order at encode time.
    let mut producer = out.input_producer;
    let inflight_pub = Arc::clone(&inflight);
    let pub_stop_p = Arc::clone(&pub_stop);
    let pace_stats = Arc::new(PaceStats::default());
    let pace_stats_pub = Arc::clone(&pace_stats);
    let publisher_core = cores.publisher;
    let publish_handle = std::thread::Builder::new()
        .name("pipeline-pub".into())
        .spawn(move || {
            if publisher_core != 0
                && let Err(e) = melin_app::affinity::pin_to_core(publisher_core)
            {
                eprintln!("warning: could not pin pipeline-pub to core {publisher_core}: {e}");
            }
            // Pacer is built inside the thread so its TSC start aligns
            // with the publisher's pinned-core clock. Pipeline mode has
            // one publisher, so `clients=1` keeps the period == the
            // aggregate target.
            let (mut pacer, warmup_end_tsc) = if target_rate > 0 {
                let start_tsc = rdtscp();
                let warmup_ticks = (phases.warmup.as_nanos() as f64 * ticks_per_ns) as u64;
                (
                    Some(PaceClock::new(target_rate, 1, ticks_per_ns, start_tsc, 0)),
                    start_tsc.saturating_add(warmup_ticks),
                )
            } else {
                (None, 0)
            };
            // Publish until the drain thread signals stop (set once the
            // cooldown deadline passes and the inflight queue is drained).
            // OrderId is a free-running u64; no risk of overflow at any
            // realistic bench duration.
            let mut i: u64 = 0;
            while !pub_stop_p.load(Ordering::Relaxed) {
                let order_id = OrderId(i + 1);
                let side = if i.is_multiple_of(2) {
                    Side::Buy
                } else {
                    Side::Sell
                };
                i += 1;

                // Spin-wait for window capacity OR a stop signal — we
                // must not block forever if the drain thread already
                // told us to stop while the window is full.
                while inflight_pub.load(Ordering::Acquire) >= window as u64 {
                    if pub_stop_p.load(Ordering::Relaxed) {
                        return;
                    }
                    std::hint::spin_loop();
                }

                // With pacing, gate on the schedule and record the
                // scheduled tick (coordinated-omission fix). Without
                // pacing, fall back to the actual send tick.
                let ts = if let Some(p) = pacer.as_mut() {
                    // Spin until the next scheduled slot is due. Done
                    // here rather than re-entering the outer loop to
                    // avoid mutating the outer order-id counter on
                    // every retry.
                    let (now_tsc, scheduled) = loop {
                        if pub_stop_p.load(Ordering::Relaxed) {
                            return;
                        }
                        let now_tsc = rdtscp();
                        if let Some(scheduled) = p.pop_due(now_tsc) {
                            break (now_tsc, scheduled);
                        }
                        std::hint::spin_loop();
                    };
                    if now_tsc >= warmup_end_tsc {
                        pace_stats_pub.record_send(now_tsc, scheduled, ticks_per_ns);
                    }
                    scheduled
                } else {
                    rdtscp()
                };
                producer.publish(InputSlot {
                    connection_id: 0,
                    key_hash: 0,
                    request_seq: 0,
                    sequence: 0,
                    timestamp_ns: tsc_clock.unix_ns(ts),
                    event: JournalEvent::App(
                        melin_trading::trading_event::TradingEvent::SubmitOrder {
                            symbol: Symbol(1),
                            order: Order {
                                id: order_id,
                                account: AccountId(1),
                                side,
                                order_type: OrderType::Limit {
                                    price: Price(nz(100)),
                                    post_only: false,
                                },
                                time_in_force: TimeInForce::GTC,
                                quantity: Quantity(nz(1)),
                                stp: SelfTradeProtection::Allow,
                                expiry_ns: 0,
                            },
                        },
                    ),
                    publish_ts: mono_trace_ns(),
                    recv_ts: mono_trace_ns(),
                });
                inflight_pub.fetch_add(1, Ordering::Release);
                ts_tx.publish(ts);
            }
        })
        .expect("spawn pipeline publish thread");

    // Drain thread (this thread): consume output SPSC and record latency.
    // It busy-spins on `try_consume` exactly like the stages do, so it
    // needs a core of its own. Left unpinned it inherits whatever the
    // process started on — core 0 on an isolcpus host, alongside the OS
    // and IRQs — and the resulting scheduling jitter lands directly in
    // the measured histogram as a millisecond-scale tail.
    if cores.drain != 0
        && let Err(e) = melin_app::affinity::pin_to_core(cores.drain)
    {
        eprintln!("warning: could not pin drain to core {}: {e}", cores.drain);
    }
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut measured_orders: u64 = 0;
    let mut measured_start: Option<Instant> = None;
    let mut outcomes = OutcomeReport::default();
    let start = phase_start;

    // Drain until cooldown ends. Classify each completion by *receive*
    // time against `deadlines`: anything within `[warmup_end, measured_end)`
    // contributes to the histogram. We don't gate the queue read on the
    // deadline — the inflight ring may still be draining when we exit.
    loop {
        let now = Instant::now();
        if now >= deadlines.cooldown_end {
            break;
        }
        let Some((_seq, slot)) = output_consumer.try_consume() else {
            std::hint::spin_loop();
            continue;
        };
        // The matching stage now signals end-of-request via the
        // `is_last_in_request` flag on the final slot for one input
        // event, instead of a separate `OutputPayload::BatchEnd` slot.
        // The flag is set on the last Report (or QueryResponse, or
        // a BatchEnd-payload slot when the event produced no payload).
        if slot.is_last_in_request {
            let (_, sent_at) = loop {
                if let Some(v) = ts_rx.try_consume() {
                    break v;
                }
                std::hint::spin_loop();
            };
            inflight.fetch_sub(1, Ordering::Release);
            // Capture `rdtscp()` BEFORE the outcome tally below so the
            // histogram reflects only the pipeline roundtrip, not the
            // bench's post-processing cost.
            let latency_ns = tsc_to_ns(rdtscp() - sent_at, ticks_per_ns);
            if now >= deadlines.warmup_end && now < deadlines.measured_end {
                if measured_start.is_none() {
                    measured_start = Some(now);
                }
                histogram.record(latency_ns).expect("record");
                measured_orders += 1;
            }
            // One request boundary per `is_last_in_request` flag — the
            // in-process equivalent of one wire `BatchEnd` frame.
            outcomes.batch_ends += 1;
        }
        // Tally the payload variant *after* the latency capture above.
        // Report payloads count as their inner execution-report variant;
        // EngineError payloads are tracked separately. BatchEnd /
        // QueryResponse payloads carry no order-acceptance signal.
        match slot.payload {
            OutputPayload::Report(report) => outcomes.record_execution_report(&report),
            OutputPayload::EngineError => outcomes.engine_errors += 1,
            OutputPayload::BatchEnd | OutputPayload::QueryResponse(_) => {}
        }
    }

    // Tell the publisher to stop and join it. The publisher checks the
    // flag both at top-of-loop and inside its window-spin, so it cannot
    // be stuck waiting forever even with a full inflight window.
    pub_stop.store(true, Ordering::Relaxed);
    publish_handle.join().expect("publisher thread");

    let end = Instant::now();
    let measured_wall = measured_start
        .map(|s| end.duration_since(s).min(phases.measured))
        .unwrap_or_else(|| start.elapsed());

    // Shutdown pipeline threads.
    shutdown.store(true, Ordering::Relaxed);

    let mut extra_lines = Vec::new();
    if group_commit_us > 0 {
        extra_lines.push(format!("  Group commit delay: {group_commit_us} µs"));
    }
    extra_lines.push(format!("  Window: {window}"));
    if target_rate > 0 {
        let scheduled = pace_stats.scheduled.load(Ordering::Relaxed);
        let late = pace_stats.late_sends.load(Ordering::Relaxed);
        let max_delay_us = tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        ) as f64
            / 1_000.0;
        extra_lines.push(format!(
            "  Target rate: {target_rate} ops/s (scheduled {scheduled}, late {late}, max send delay {max_delay_us:.1} µs)"
        ));
    }

    let pacing_report = if target_rate > 0 {
        let max_delay_ns = tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        );
        Some(PacingReport {
            target_rate,
            scheduled: pace_stats.scheduled.load(Ordering::Relaxed),
            late_sends: pace_stats.late_sends.load(Ordering::Relaxed),
            max_send_delay_us: max_delay_ns as f64 / 1_000.0,
        })
    } else {
        None
    };

    print_results(RunReport {
        label: "Pipeline (no network)",
        unit: &ORDER,
        measured_count: measured_orders as usize,
        phases,
        histogram: &histogram,
        wall: measured_wall,
        extra_lines: &extra_lines,
        json_path,
        series: &Vec::new(),
        health: &health::HealthReport::default(),
        // Pipeline mode runs the disruptor stages in-process with no
        // server / health endpoint, so there's nothing to fetch.
        server_stages: &stats::Body::Empty,
        pacing: pacing_report.as_ref(),
        outcomes: &outcomes,
    });

    enforce_rejection_threshold(&outcomes, max_reject_pct);

    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();

    // Wait for pipeline threads to finish and print trace reports.
    let _ = journal_handle.join();
    let _ = matching_handle.join();
    // No embedded server in pipeline mode, so nothing else dumps the
    // stage registry — print it here (no-op without `latency-trace`).
    melin_transport_core::trace::print_report_all();
}

// ===========================================================================
// Roundtrip benchmark (full server with network transport)
// ===========================================================================

/// Full end-to-end roundtrip benchmark through the server with TCP or UDS.
///
/// When `remote_addr` is `Some`, connects to a remote engine instead of
/// spawning an embedded server. This is the mode used for LAN benchmarks
/// where the engine runs on a separate machine.
#[allow(clippy::too_many_arguments)]
#[cfg(not(feature = "dpdk"))]
fn run_roundtrip_bench(
    use_uds: bool,
    phases: BenchPhases,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
    remote_addr: Option<std::net::SocketAddr>,
    journal_path: Option<std::path::PathBuf>,
    num_accounts: u32,
    num_instruments: u32,
    json_path: Option<&std::path::Path>,
    key_path: Option<&std::path::Path>,
    bench_core_start: Option<usize>,
    health_addr: Option<std::net::SocketAddr>,
    target_rate: u64,
    max_reject_pct: f64,
) {
    // Remote mode: connect to an external engine, no embedded server.
    if let Some(addr) = remote_addr {
        if use_uds {
            eprintln!("error: --addr and --uds are mutually exclusive");
            std::process::exit(1);
        }

        let key_path = key_path.unwrap_or_else(|| {
            eprintln!("error: --key is required for remote mode (--addr)");
            std::process::exit(1);
        });
        let key = load_signing_key(key_path);
        let shutdown = Arc::new(AtomicBool::new(false));

        let connect = || {
            let stream = connect_tcp(addr);
            stream.set_nodelay(true).expect("set TCP_NODELAY");
            let read_stream = stream.try_clone().expect("clone TCP stream");
            (read_stream, stream)
        };

        run_roundtrip_inner(
            connect,
            &format!("TCP {addr}"),
            phases,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            json_path,
            &key,
            num_accounts,
            num_instruments,
            bench_core_start,
            health_addr,
            target_rate,
            max_reject_pct,
        );
        return;
    }

    // Local mode: spawn an embedded server.
    // Generate a deterministic bench master key and write the N derived
    // per-client pubkeys to authorized_keys — every client connection
    // authenticates with its own derived key so the engine's per-key
    // dedup HWM partitions cleanly across connections.
    let bench_key = ed25519_dalek::SigningKey::from_bytes(&[0xBE; 32]);
    let tmp_dir = tempdir();
    let keys_path = tmp_dir.join("authorized_keys");
    let authorized_keys = (0..num_clients)
        .map(|i| {
            let child = keys::derive_client_key(&bench_key, i as u32);
            format!(
                "{}\n",
                keys::authorized_keys_line(&child, &format!("bench-{i}"))
            )
        })
        .collect::<String>();
    std::fs::write(&keys_path, authorized_keys).expect("write authorized_keys");

    let effective_journal = journal_path.unwrap_or_else(|| tmp_dir.join("bench.journal"));

    let config = ServerConfig {
        journal: effective_journal,
        snapshot: None,
        group_commit_us,
        accounts: num_accounts,
        instruments: num_instruments,
        // Disable connection timeout for benchmarks — pre-generation
        // can take longer than the default 30s for large runs.
        connection_timeout_secs: 0,
        authorized_keys: keys_path,
        // Single-node durability for the embedded bench server: ack on
        // local persistence alone. The default `Hybrid` mode waits for
        // `in_memory>=2` replica acks that never arrive when nothing else
        // is connected, which would stall every response.
        durability_mode: melin_server_runtime::durability_policy::DurabilityMode::Local,
        ..ServerConfig::default()
    };
    // Wire the trading AppFactory: replication / seed paths take it
    // as an argument to `run_with_listener`. The bench server runs
    // standalone but still bulk-seeds via the same code path as the
    // binary, so the factory must be constructed even for in-process
    // benchmarks.
    let factory =
        melin_server::app_factory::Factory::new(melin_server::app_factory::FactoryConfig {
            accounts: config.accounts,
            instruments: config.instruments,
            max_orders_per_account: config.max_orders_per_account,
            max_orders_per_second: config.max_orders_per_second,
            max_orders_burst: config.max_orders_burst,
        });

    let shutdown = Arc::new(AtomicBool::new(false));

    // Capture health bind address before config is moved into the server thread.
    let effective_health_addr = health_addr.or(config.health_bind);

    if use_uds {
        use melin_wire_protocol::uds::BlockingUdsListener;

        let sock_path = tmp_dir.join("bench.sock");
        let listener = BlockingUdsListener::bind(&sock_path).expect("bind UDS");
        start_server(listener, config, factory, Arc::clone(&shutdown));

        let sock_path_ref = &sock_path;
        let connect = || {
            let stream = connect_uds(sock_path_ref);
            let read_stream = stream.try_clone().expect("clone UDS stream");
            (read_stream, stream)
        };

        run_roundtrip_inner(
            connect,
            "Unix domain socket",
            phases,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            json_path,
            &bench_key,
            num_accounts,
            num_instruments,
            bench_core_start,
            effective_health_addr,
            target_rate,
            max_reject_pct,
        );
    } else {
        use melin_wire_protocol::tcp::BlockingTcpListener;

        let listener = BlockingTcpListener::bind("127.0.0.1:0".parse().expect("valid addr"))
            .expect("bind TCP");
        let addr = listener.local_addr().expect("local addr");
        start_server(listener, config, factory, Arc::clone(&shutdown));

        let connect = || {
            let stream = connect_tcp(addr);
            stream.set_nodelay(true).expect("set TCP_NODELAY");
            let read_stream = stream.try_clone().expect("clone TCP stream");
            (read_stream, stream)
        };

        run_roundtrip_inner(
            connect,
            "TCP loopback",
            phases,
            window,
            num_clients,
            bench_threads,
            group_commit_us,
            shutdown,
            json_path,
            &bench_key,
            num_accounts,
            num_instruments,
            bench_core_start,
            effective_health_addr,
            target_rate,
            max_reject_pct,
        );
    }

    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Load a 32-byte raw Ed25519 private key from a file.
fn load_signing_key(path: &std::path::Path) -> ed25519_dalek::SigningKey {
    let bytes = std::fs::read(path)
        .unwrap_or_else(|e| panic!("cannot read key file {}: {e}", path.display()));
    if bytes.len() != 32 {
        panic!(
            "key file must be exactly 32 bytes (raw Ed25519 seed), got {}",
            bytes.len()
        );
    }
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&bytes);
    ed25519_dalek::SigningKey::from_bytes(&seed)
}

/// Start the server on a background thread. The listener is already bound,
/// so the client can connect immediately (connections queue in the kernel
/// backlog until the server calls `accept()`).
#[cfg(not(feature = "dpdk"))]
fn start_server<L: BlockingTransportListener>(
    listener: L,
    config: ServerConfig,
    factory: melin_server::app_factory::Factory,
    shutdown: Arc<AtomicBool>,
) {
    use melin_server::request_decoder::RequestDecoder;
    use melin_server::response_encoder::ResponseEncoder;
    use melin_server_runtime::server::EventPublisherFn;

    let event_publisher: Option<EventPublisherFn<ServerApp>> = None;
    std::thread::Builder::new()
        .name("server".into())
        .spawn(move || {
            if let Err(e) = melin_server_runtime::server::run_with_listener(
                listener,
                config,
                factory,
                RequestDecoder,
                ResponseEncoder,
                event_publisher,
                shutdown,
            ) {
                eprintln!("server error: {e}");
            }
        })
        .expect("spawn server thread");
}

/// Perform challenge-response auth handshake on a new connection.
/// Must be called before the stream is set to non-blocking mode.
#[cfg(not(feature = "dpdk"))]
fn auth_handshake(
    stream: &mut (impl std::io::Read + std::io::Write),
    key: &ed25519_dalek::SigningKey,
) {
    use ed25519_dalek::Signer;
    use melin_protocol::message::Request;

    // Read Challenge frame.
    let mut len_buf = [0u8; 4];
    std::io::Read::read_exact(stream, &mut len_buf).expect("read Challenge length");
    let len = u32::from_le_bytes(len_buf) as usize;
    assert!(len <= MAX_FRAME_SIZE, "Challenge frame too large: {len}");
    let mut payload = [0u8; 128];
    std::io::Read::read_exact(stream, &mut payload[..len]).expect("read Challenge payload");
    let response = codec::decode_response(&payload[..len]).expect("decode Challenge");
    let nonce = match response {
        ResponseKind::Challenge { nonce } => nonce,
        other => panic!("expected Challenge, got {other:?}"),
    };

    let signature = key.sign(&nonce);
    let request = Request::ChallengeResponse {
        signature: signature.to_bytes(),
        public_key: key.verifying_key().to_bytes(),
    };
    let mut buf = [0u8; 256];
    let written = codec::encode_request(&request, 0, &mut buf).expect("encode ChallengeResponse");
    std::io::Write::write_all(stream, &buf[..written]).expect("send ChallengeResponse");
    std::io::Write::flush(stream).expect("flush ChallengeResponse");

    // Read ServerReady.
    std::io::Read::read_exact(stream, &mut len_buf).expect("read ServerReady length");
    let len = u32::from_le_bytes(len_buf) as usize;
    assert!(len <= MAX_FRAME_SIZE, "ServerReady frame too large: {len}");
    std::io::Read::read_exact(stream, &mut payload[..len]).expect("read ServerReady payload");
    let response = codec::decode_response(&payload[..len]).expect("decode ServerReady");
    assert!(
        matches!(response, ResponseKind::ServerReady),
        "expected ServerReady, got {response:?}"
    );
}

// Orchestration
// ---------------------------------------------------------------------------

/// Create connections, distribute across bench threads, run, report results.
#[allow(clippy::too_many_arguments)]
#[cfg(not(feature = "dpdk"))]
fn run_roundtrip_inner<R, W, F>(
    connect: F,
    transport_name: &str,
    phases: BenchPhases,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
    num_accounts: u32,
    num_instruments: u32,
    bench_core_start: Option<usize>,
    health_addr: Option<std::net::SocketAddr>,
    target_rate: u64,
    max_reject_pct: f64,
) where
    R: std::io::Read + std::io::Write + AsRawFd + Send + 'static,
    W: Write + AsRawFd + Send + 'static,
    F: Fn() -> (R, W) + Sync,
{
    run_uring_roundtrip(
        connect,
        transport_name,
        phases,
        window,
        num_clients,
        bench_threads,
        group_commit_us,
        shutdown,
        json_path,
        key,
        num_accounts,
        num_instruments,
        bench_core_start,
        health_addr,
        target_rate,
        max_reject_pct,
    );
}

// ===========================================================================
// Progress reporting
// ===========================================================================

/// Spawn a background thread that prints periodic progress to stderr.
/// Returns a handle; the thread exits when `shutdown` is set to true.
///
/// Pinned to core 0 (OS/IRQ core) so it never preempts bench I/O threads.
/// Uses raw `write(2)` on fd 2 instead of `eprintln!` to avoid the stderr
/// mutex, which can block bench threads that also write to stderr.
pub(crate) fn spawn_progress_reporter(
    completed: Arc<AtomicU64>,
    phases: BenchPhases,
    shutdown: Arc<AtomicBool>,
    target_rate: u64,
    pace_stats: Arc<PaceStats>,
) -> std::thread::JoinHandle<()> {
    let total_duration = phases.warmup + phases.measured + phases.cooldown;
    std::thread::Builder::new()
        .name("progress".into())
        .spawn(move || {
            // Pin to core 0 so the progress thread never lands on a bench
            // core and causes involuntary preemption or TLB shootdowns.
            let _ = melin_app::affinity::pin_to_core(0);

            let start = Instant::now();
            let mut last_completed: u64 = 0;
            let mut last_time = start;
            // Print interval is 5s, but poll the shutdown flag every 100ms so
            // bench cleanup doesn't have to wait the full interval to exit.
            const PRINT_INTERVAL: Duration = Duration::from_secs(5);
            const POLL_INTERVAL: Duration = Duration::from_millis(100);

            'outer: loop {
                let mut waited = Duration::ZERO;
                while waited < PRINT_INTERVAL {
                    if shutdown.load(Ordering::Relaxed) {
                        break 'outer;
                    }
                    std::thread::sleep(POLL_INTERVAL);
                    waited += POLL_INTERVAL;
                }

                let now = Instant::now();
                let current = completed.load(Ordering::Relaxed);
                let dt = now.duration_since(last_time).as_secs_f64();
                let delta = current.saturating_sub(last_completed);
                let rate = delta as f64 / dt;
                let elapsed = now.duration_since(start).as_secs_f64();
                let total_secs = total_duration.as_secs_f64();
                let pct = if total_secs > 0.0 {
                    (elapsed / total_secs * 100.0).min(100.0)
                } else {
                    100.0
                };
                let phase = if elapsed < phases.warmup.as_secs_f64() {
                    "warmup"
                } else if elapsed < (phases.warmup + phases.measured).as_secs_f64() {
                    "measured"
                } else {
                    "cooldown"
                };

                // Format into a stack buffer and write(2) directly to fd 2.
                // Avoids the stderr mutex that eprintln! holds, which can
                // block bench threads doing eprintln! on error paths.
                use std::io::Write as _;
                let mut buf = [0u8; 256];
                let mut cursor = std::io::Cursor::new(&mut buf[..]);
                if target_rate > 0 {
                    let scheduled = pace_stats.scheduled.load(Ordering::Relaxed);
                    let late = pace_stats.late_sends.load(Ordering::Relaxed);
                    let _ = writeln!(
                        cursor,
                        "  [{elapsed:.1}s/{total_secs:.0}s {pct:.0}% {phase}] scheduled {scheduled} / done {current} / late {late}  {:.0}K/s",
                        rate / 1000.0,
                    );
                } else {
                    let _ = writeln!(
                        cursor,
                        "  [{elapsed:.1}s/{total_secs:.0}s {pct:.0}% {phase}] {current} measured orders  {:.0}K/s",
                        rate / 1000.0,
                    );
                }
                let len = cursor.position() as usize;
                // Best-effort write — progress display is non-critical.
                unsafe {
                    libc::write(2, buf.as_ptr() as *const libc::c_void, len);
                }

                last_completed = current;
                last_time = now;
            }
        })
        .expect("spawn progress thread")
}

// ===========================================================================
// io_uring roundtrip benchmark
// ===========================================================================

/// io_uring-based roundtrip benchmark. Each bench thread runs its own
/// io_uring ring with RECV for reads and SEND for writes.
#[cfg(not(feature = "dpdk"))]
#[allow(clippy::too_many_arguments)]
fn run_uring_roundtrip<R, W, F>(
    connect: F,
    transport_name: &str,
    phases: BenchPhases,
    window: usize,
    num_clients: usize,
    bench_threads: usize,
    group_commit_us: u64,
    shutdown: Arc<AtomicBool>,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
    num_accounts: u32,
    num_instruments: u32,
    bench_core_start: Option<usize>,
    health_addr: Option<std::net::SocketAddr>,
    target_rate: u64,
    max_reject_pct: f64,
) where
    R: std::io::Read + std::io::Write + AsRawFd + Send + 'static,
    W: Write + AsRawFd + Send + 'static,
    F: Fn() -> (R, W) + Sync,
{
    // Build a generator per client. With on-the-fly generation the loop
    // is never starved by a pre-allocated cap; phases are driven entirely
    // by the wall-clock deadlines defined by `phases`. Each generator
    // gets a non-overlapping `start_order_id` slice from `OrderId` space.
    //
    // `ORDER_ID_STRIDE` reserves a generous block per client so a long
    // bench at 10 M/s (≈ 6e11 orders/min) still fits a u64 slot without
    // colliding across clients. 2^48 ≈ 2.8e14 ids — three orders of
    // magnitude beyond any realistic run.
    const ORDER_ID_STRIDE: u64 = 1u64 << 48;
    let per_client: Vec<generator::OrderFlowGenerator> = (0..num_clients)
        .map(|client_id| {
            generator::OrderFlowGenerator::new(generator::GeneratorConfig {
                num_accounts,
                num_instruments,
                start_order_id: ORDER_ID_STRIDE * (client_id as u64) + 1,
                ..Default::default()
            })
        })
        .collect();
    eprintln!("  per-client generators initialised for {num_clients} clients");

    // Per-client signing keys: the engine dedups by `(key_hash,
    // request_seq)` with a single per-key HWM that only advances. If
    // every connection shared one key, the leading client's seq would
    // jump the HWM and every other connection's request would be
    // rejected as `DuplicateRequest`. Deriving a child key per client
    // gives each connection its own `key_hash`, so dedup is partitioned
    // per connection and seqs can grow independently.
    let client_keys: Vec<ed25519_dalek::SigningKey> = (0..num_clients)
        .map(|i| keys::derive_client_key(key, i as u32))
        .collect();

    // Connect and auth all clients in parallel via rayon — independent
    // network handshakes that amortise nicely across a thread pool.
    use rayon::prelude::*;
    let setup_start = Instant::now();
    let connected: Vec<(R, W)> = (0..num_clients)
        .into_par_iter()
        .map(|i| {
            let (mut read_stream, write_stream) = connect();
            auth_handshake(&mut read_stream, &client_keys[i]);
            (read_stream, write_stream)
        })
        .collect();
    eprintln!(
        "  all {num_clients} clients connected ({:.1}s)",
        setup_start.elapsed().as_secs_f64(),
    );

    let num_threads = bench_threads.min(num_clients);

    // Attach per-client generator and distribute round-robin across bench threads.
    let mut thread_conns: Vec<Vec<Connection<ExchangeWorkload>>> =
        (0..num_threads).map(|_| Vec::new()).collect();
    for (i, ((read_stream, write_stream), flow)) in
        connected.into_iter().zip(per_client).enumerate()
    {
        thread_conns[i % num_threads].push(Connection::new(
            read_stream,
            write_stream,
            ExchangeWorkload::new(flow),
            window,
        ));
    }

    let progress = Arc::new(AtomicU64::new(0));
    let progress_shutdown = Arc::new(AtomicBool::new(false));
    let pace_stats = Arc::new(PaceStats::default());
    let progress_handle = spawn_progress_reporter(
        Arc::clone(&progress),
        phases,
        Arc::clone(&progress_shutdown),
        target_rate,
        Arc::clone(&pace_stats),
    );

    // Start health poller before bench threads.
    let health_poller = health_addr.map(health::HealthPoller::start);

    // Shared start instant — every bench thread derives its phase
    // deadlines from this so they classify completions consistently.
    let start = Instant::now();
    let deadlines = phases.deadlines(start);

    // Spawn io_uring bench threads, each with its own ring and connection subset.
    let handles: Vec<_> = thread_conns
        .into_iter()
        .enumerate()
        .map(|(i, conns)| {
            let pin_core = bench_core_start.map(|s| s + i);
            let bench_start = start;
            let thread_progress = Arc::clone(&progress);
            let thread_pace_stats = Arc::clone(&pace_stats);
            // Global-conn-index mapping mirrors the round-robin
            // distribution above (`thread_conns[i % num_threads]`):
            // this thread's local conn `k` is global conn
            // `thread_idx + k * num_threads`. Passed in so the pacer
            // stagger spreads first sends across *all* connections, not
            // just within each thread.
            let thread_idx = i;
            let total_threads = num_threads;
            std::thread::Builder::new()
                .name(format!("bench-{i}"))
                .spawn(move || {
                    if let Some(core_id) = pin_core
                        && let Err(e) = melin_app::affinity::pin_to_core(core_id)
                    {
                        eprintln!("warning: could not pin bench-{i} to core {core_id}: {e}");
                    }
                    uring::run_loop(
                        conns,
                        LoopConfig {
                            window,
                            bench_start,
                            deadlines,
                            phases,
                            progress: thread_progress,
                            target_rate,
                            total_clients: num_clients,
                            thread_idx,
                            total_threads,
                            pace_stats: thread_pace_stats,
                        },
                    )
                })
                .expect("spawn bench thread")
        })
        .collect();

    // Collect and merge histograms from all threads. Track the earliest
    // measured_start — measurement begins when the first thread exits
    // warmup, so the wall time covers all measured orders from all threads.
    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut earliest_measured_start: Option<Instant> = None;
    let mut all_series: TimeSeries = Vec::new();
    let mut outcomes = OutcomeReport::default();

    for handle in handles {
        let uring::LoopResult {
            histogram: h,
            series: s,
            measured_start: ms,
            outcomes: o,
        } = handle.join().expect("bench thread panicked");
        histogram.add(&h).expect("merge histograms");
        if let Some(t) = ms {
            earliest_measured_start =
                Some(earliest_measured_start.map_or(t, |prev: Instant| prev.min(t)));
        }
        all_series.extend(s);
        outcomes.merge(&o);
    }

    // Snapshot end time BEFORE joining the progress thread: that thread
    // sleeps in 5-second increments and only checks shutdown after each
    // sleep, so progress_handle.join() can block up to ~5s and would
    // otherwise inflate `measured_wall` for short benches.
    let end = Instant::now();

    // Stop progress reporter.
    progress_shutdown.store(true, Ordering::Relaxed);
    let _ = progress_handle.join();

    // Collect health samples.
    let health = health_poller.map(|p| p.stop()).unwrap_or_default();

    // Measure throughput over the measured phase only — from when the
    // first thread finished warmup until either `end` (captured above,
    // pre-join) or `start + warmup + measured`, whichever is sooner.
    // `end` lands inside cooldown when threads exited via the wall-clock
    // deadline, so capping at `phases.measured` keeps the divisor
    // honest.
    let measured_wall = earliest_measured_start
        .map(|s| end.duration_since(s).min(phases.measured))
        .unwrap_or_else(|| start.elapsed());

    let mut extra_lines = Vec::new();
    if group_commit_us > 0 {
        extra_lines.push(format!("  Group commit delay: {group_commit_us} µs"));
    }
    extra_lines.push(format!("  Transport: {transport_name}"));
    extra_lines.push(if let Some(start) = bench_core_start {
        format!(
            "  Bench threads: {num_threads} (io_uring, cores {start}-{})",
            start + num_threads - 1,
        )
    } else {
        format!("  Bench threads: {num_threads} (io_uring, unpinned)")
    });
    extra_lines.push(format!("  Window: {window}, Clients: {num_clients}"));

    // Calibrate once for both the human-readable line and the JSON
    // report; calibration sleeps ~50 ms so calling it twice is wasteful.
    // TSC drift between bench threads on the same socket is well below
    // µs, so a single calibration here is fine for the report.
    let pacing_report = if target_rate > 0 {
        let ticks_per_ns = calibrate_tsc();
        let scheduled = pace_stats.scheduled.load(Ordering::Relaxed);
        let late = pace_stats.late_sends.load(Ordering::Relaxed);
        let max_delay_us = tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        ) as f64
            / 1_000.0;
        extra_lines.push(format!(
            "  Target rate: {target_rate} ops/s (scheduled {scheduled}, late {late}, max send delay {max_delay_us:.1} µs)"
        ));
        Some(PacingReport {
            target_rate,
            scheduled,
            late_sends: late,
            max_send_delay_us: max_delay_us,
        })
    } else {
        None
    };

    // Sort time-series by elapsed time for stable plot output.
    all_series.sort_by(|a, b| a.elapsed_secs.partial_cmp(&b.elapsed_secs).unwrap());

    // Fetch the server-side per-stage histogram dump before the
    // server (or its embedded form) shuts down. Best-effort — a
    // missing dump never aborts the run; print_results renders an
    // appropriate "feature off" / "no data" line instead.
    let server_stages = match health_addr {
        Some(addr) => stats::fetch(addr),
        None => stats::Body::Empty,
    };

    print_results(RunReport {
        label: "Roundtrip",
        unit: &ORDER,
        measured_count: histogram.len() as usize,
        phases,
        histogram: &histogram,
        wall: measured_wall,
        extra_lines: &extra_lines,
        json_path,
        series: &all_series,
        health: &health,
        server_stages: &server_stages,
        pacing: pacing_report.as_ref(),
        outcomes: &outcomes,
    });

    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();
    shutdown.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(200));

    enforce_rejection_threshold(&outcomes, max_reject_pct);
}

/// Create a temporary directory that persists for the process lifetime.
fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("melin-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[cfg(test)]
mod pipeline_core_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn five_entries_map_in_documented_order() {
        let cores = resolve_pipeline_cores(&[1, 2, 3, 4, 5]).unwrap();
        assert_eq!(
            cores,
            PipelineBenchCores {
                journal: 1,
                matching: 2,
                publisher: 3,
                journal_disk: 4,
                drain: 5,
            }
        );
    }

    #[test]
    fn the_default_flag_value_parses() {
        // Guards the `default_value` string on `--pipeline-cores` against
        // drifting out of sync with `PIPELINE_CORE_SLOTS`.
        let parsed = BenchArgs::parse_from(["melin-bench"]).pipeline_cores;
        assert!(resolve_pipeline_cores(&parsed).is_ok(), "{parsed:?}");
    }

    #[test]
    fn wrong_arity_is_rejected() {
        let err = resolve_pipeline_cores(&[1, 2, 3, 4]).unwrap_err();
        assert!(err.contains("expects 5"), "{err}");
    }

    #[test]
    fn a_duplicate_core_names_both_claimants() {
        // The collision the old hardcoded layout invited: the drain
        // thread landing on the journal's disk core.
        let err = resolve_pipeline_cores(&[1, 2, 3, 4, 4]).unwrap_err();
        assert!(err.contains("core 4"), "{err}");
        assert!(err.contains("journal-disk"), "{err}");
        assert!(err.contains("drain"), "{err}");
    }

    #[test]
    fn zero_is_an_unpinned_sentinel_not_a_core() {
        // Several threads may be left unpinned at once; 0 must not read
        // as a duplicate claim on core 0.
        let cores = resolve_pipeline_cores(&[1, 0, 0, 0, 0]).unwrap();
        assert_eq!(cores.journal, 1);
        assert_eq!(cores.drain, 0);
    }
}
