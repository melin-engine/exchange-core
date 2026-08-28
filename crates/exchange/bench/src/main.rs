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
#[cfg(not(feature = "dpdk"))]
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
// The kernel-transport path hands its connections to the harness's
// io_uring loop; the DPDK path runs its own loop over smoltcp sockets.
use melin_bench_harness::series::{LatencySample, TimeSeries, maybe_sample};
#[cfg(not(feature = "dpdk"))]
use melin_bench_harness::transport::{connect_tcp, connect_uds};
#[cfg(not(feature = "dpdk"))]
use melin_bench_harness::uring::{self, Connection, LoopConfig};
use melin_bench_harness::{health, keys, series, stats};
#[cfg(not(feature = "dpdk"))]
use workload::ExchangeWorkload;

#[cfg(not(feature = "dpdk"))]
use melin_protocol::codec;
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

    print_results(
        "Realistic Order Flow",
        measured_orders as usize,
        phases,
        &histogram,
        wall,
        &extra_lines,
        json_path,
        &series,
        &health::HealthReport::default(),
        // Engine mode runs the matching engine in-process with no
        // server / health endpoint, so there's nothing to fetch.
        &stats::Body::Empty,
        pacing_report.as_ref(),
        Some(&outcomes),
    );

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

    print_results(
        "Pipeline (no network)",
        measured_orders as usize,
        phases,
        &histogram,
        measured_wall,
        &extra_lines,
        json_path,
        &Vec::new(),
        &health::HealthReport::default(),
        // Pipeline mode runs the disruptor stages in-process with no
        // server / health endpoint, so there's nothing to fetch.
        &stats::Body::Empty,
        pacing_report.as_ref(),
        Some(&outcomes),
    );

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

    print_results(
        "Roundtrip",
        histogram.len() as usize,
        phases,
        &histogram,
        measured_wall,
        &extra_lines,
        json_path,
        &all_series,
        &health,
        &server_stages,
        pacing_report.as_ref(),
        Some(&outcomes),
    );

    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();
    shutdown.store(true, Ordering::Relaxed);
    std::thread::sleep(Duration::from_millis(200));

    enforce_rejection_threshold(&outcomes, max_reject_pct);
}

// ===========================================================================
// Shared reporting
// ===========================================================================

/// Print a latency histogram in µs. Adaptive nines: only prints p99.9, p99.99,
/// etc. when `sample_count` is large enough (10×  per extra nine).
pub(crate) fn print_latency_histogram(hist: &Histogram<u64>, sample_count: usize) {
    println!("    min:     {:>8.2} µs", hist.min() as f64 / 1_000.0);
    println!(
        "    p50:     {:>8.2} µs",
        hist.value_at_quantile(0.50) as f64 / 1_000.0
    );
    println!(
        "    p90:     {:>8.2} µs",
        hist.value_at_quantile(0.90) as f64 / 1_000.0
    );
    let mut nines = 2;
    let mut threshold = 1_000usize;
    while threshold <= sample_count {
        let quantile = 1.0 - 10.0f64.powi(-(nines as i32));
        let label = if nines <= 2 {
            "p99".to_string()
        } else {
            format!("p99.{}", "9".repeat(nines - 2))
        };
        let value = hist.value_at_quantile(quantile) as f64 / 1_000.0;
        let padded = format!("{label}:");
        println!("    {padded:<9}{value:>8.2} µs");
        nines += 1;
        threshold *= 10;
    }
    println!("    max:     {:>8.2} µs", hist.max() as f64 / 1_000.0);
}

/// Stable ordering of [`RejectReason`] variants used as the index space
/// for [`OutcomeReport::reject_reasons`]. Adding a new reject variant is
/// a compile error inside [`reject_reason_index`] until the entry is
/// appended here too — keep the two in sync.
pub(crate) const REJECT_REASONS: &[(RejectReason, &str)] = &[
    (RejectReason::NoLiquidity, "NoLiquidity"),
    (RejectReason::FOKCannotFill, "FOKCannotFill"),
    (RejectReason::InsufficientBalance, "InsufficientBalance"),
    (RejectReason::UnknownAccount, "UnknownAccount"),
    (RejectReason::UnknownSymbol, "UnknownSymbol"),
    (RejectReason::SelfTradePrevented, "SelfTradePrevented"),
    (RejectReason::DuplicateOrderId, "DuplicateOrderId"),
    (RejectReason::ExceedsMaxOrderQty, "ExceedsMaxOrderQty"),
    (RejectReason::ExceedsMaxNotional, "ExceedsMaxNotional"),
    (RejectReason::TradingHalted, "TradingHalted"),
    (RejectReason::OutsidePriceBand, "OutsidePriceBand"),
    (RejectReason::UnknownOrder, "UnknownOrder"),
    (RejectReason::PriceWouldCross, "PriceWouldCross"),
    (RejectReason::PostOnlyWouldCross, "PostOnlyWouldCross"),
    (RejectReason::HasRestingOrders, "HasRestingOrders"),
    (RejectReason::DuplicateRequest, "DuplicateRequest"),
    (RejectReason::ReplicaDisconnected, "ReplicaDisconnected"),
    (RejectReason::InvalidExpiry, "InvalidExpiry"),
    (RejectReason::InstrumentDisabled, "InstrumentDisabled"),
    (RejectReason::ExceedsMaxOpenOrders, "ExceedsMaxOpenOrders"),
    (RejectReason::ExceedsOrderRate, "ExceedsOrderRate"),
    (RejectReason::Superseded, "Superseded"),
];

fn reject_reason_index(reason: RejectReason) -> usize {
    // `RejectReason` is not `#[repr(u8)]`, so the discriminant isn't a
    // stable index. An exhaustive match makes adding a new variant a
    // compile error until both this function and `REJECT_REASONS` above
    // are updated.
    let idx = match reason {
        RejectReason::NoLiquidity => 0,
        RejectReason::FOKCannotFill => 1,
        RejectReason::InsufficientBalance => 2,
        RejectReason::UnknownAccount => 3,
        RejectReason::UnknownSymbol => 4,
        RejectReason::SelfTradePrevented => 5,
        RejectReason::DuplicateOrderId => 6,
        RejectReason::ExceedsMaxOrderQty => 7,
        RejectReason::ExceedsMaxNotional => 8,
        RejectReason::TradingHalted => 9,
        RejectReason::OutsidePriceBand => 10,
        RejectReason::UnknownOrder => 11,
        RejectReason::PriceWouldCross => 12,
        RejectReason::PostOnlyWouldCross => 13,
        RejectReason::HasRestingOrders => 14,
        RejectReason::DuplicateRequest => 15,
        RejectReason::ReplicaDisconnected => 16,
        RejectReason::InvalidExpiry => 17,
        RejectReason::InstrumentDisabled => 18,
        RejectReason::ExceedsMaxOpenOrders => 19,
        RejectReason::ExceedsOrderRate => 20,
        RejectReason::Superseded => 21,
    };
    // Catch silent label/index swaps: an exhaustive match would still
    // type-check if two arms had their integers swapped, but the
    // `REJECT_REASONS` table would then mislabel counts at print time.
    // The existing `reject_reasons_indices_are_unique_and_match_table_length`
    // test calls this for every variant, so a swap explodes there.
    debug_assert_eq!(
        REJECT_REASONS[idx].0, reason,
        "REJECT_REASONS table and reject_reason_index match arms diverged at idx {idx}",
    );
    idx
}

/// Counts of execution-report variants observed by the bench client over
/// the lifetime of a run. Folded across connections and bench threads to
/// surface the rejection ratio in the run summary — without this, a
/// misconfigured run where every order is rejected looks identical to a
/// clean run in the latency histogram.
///
/// Plain `u64` fields (not atomics) because each connection is owned by
/// a single bench thread; merging happens after thread join.
#[derive(Default, Clone)]
pub(crate) struct OutcomeReport {
    /// `BatchEnd` frames received — one per acknowledged request, so
    /// this is the denominator for the rejection ratio.
    pub batch_ends: u64,
    pub placed: u64,
    pub fills: u64,
    pub cancelled: u64,
    pub triggered: u64,
    pub replaced: u64,
    pub instrument_status: u64,
    pub rejected: u64,
    pub engine_errors: u64,
    pub server_busy: u64,
    /// Per-reason rejection counts. Index space defined by
    /// [`REJECT_REASONS`] / [`reject_reason_index`].
    pub reject_reasons: [u64; REJECT_REASONS.len()],
}

impl OutcomeReport {
    /// Increment the counter that matches `response`. Untracked variants
    /// (handshake / market-data / stats frames) are ignored.
    #[inline]
    pub fn record(&mut self, response: &ResponseKind) {
        match response {
            ResponseKind::BatchEnd => self.batch_ends += 1,
            ResponseKind::Report(report) => self.record_execution_report(report),
            ResponseKind::EngineError => self.engine_errors += 1,
            ResponseKind::ServerBusy => self.server_busy += 1,
            // Non-trading frames (Challenge, ServerReady, Heartbeat,
            // AuthFailed, stats/market-data snapshots) — not part of the
            // request/ack accounting.
            _ => {}
        }
    }

    /// Increment the counter for a single execution-report variant.
    /// Used by in-process bench modes (engine, pipeline) which observe
    /// the matching stage's reports directly without going through the
    /// wire `ResponseKind::Report` wrapper.
    #[inline]
    pub fn record_execution_report(&mut self, report: &ExecutionReport) {
        match report {
            ExecutionReport::Placed { .. } => self.placed += 1,
            ExecutionReport::Fill { .. } => self.fills += 1,
            ExecutionReport::Cancelled { .. } => self.cancelled += 1,
            ExecutionReport::Triggered { .. } => self.triggered += 1,
            ExecutionReport::Replaced { .. } => self.replaced += 1,
            ExecutionReport::InstrumentStatusChanged { .. } => self.instrument_status += 1,
            ExecutionReport::Rejected { reason, .. } => {
                self.rejected += 1;
                self.reject_reasons[reject_reason_index(*reason)] += 1;
            }
        }
    }

    pub fn merge(&mut self, other: &OutcomeReport) {
        self.batch_ends += other.batch_ends;
        self.placed += other.placed;
        self.fills += other.fills;
        self.cancelled += other.cancelled;
        self.triggered += other.triggered;
        self.replaced += other.replaced;
        self.instrument_status += other.instrument_status;
        self.rejected += other.rejected;
        self.engine_errors += other.engine_errors;
        self.server_busy += other.server_busy;
        for (a, b) in self
            .reject_reasons
            .iter_mut()
            .zip(other.reject_reasons.iter())
        {
            *a += *b;
        }
    }

    /// Fraction of acknowledged requests that were rejected. Returns
    /// `0.0` when no batches were observed, so callers comparing against
    /// a threshold treat a zero-response run as "no rejections seen"
    /// rather than 100% — a stalled run is a separate failure mode and
    /// is already surfaced by the throughput line.
    pub fn rejection_ratio(&self) -> f64 {
        if self.batch_ends == 0 {
            0.0
        } else {
            self.rejected as f64 / self.batch_ends as f64
        }
    }
}

/// Fail the run with a non-zero exit if more than `max_pct` percent of
/// acknowledged requests were rejected. The CLI default is 50% — the
/// generator naturally produces a few percent of rejections, so the
/// gate targets catastrophic misconfig ("most orders rejected") rather
/// than noise. Lower `max_pct` for production-flow runs where rejections
/// should be near-zero; set it to 100.0 to disable.
pub(crate) fn enforce_rejection_threshold(outcomes: &OutcomeReport, max_pct: f64) {
    let pct = outcomes.rejection_ratio() * 100.0;
    if pct > max_pct {
        eprintln!(
            "error: rejection ratio {pct:.2}% exceeds --max-reject-pct {max_pct:.2}% \
             ({} rejected of {} acknowledged requests). Likely a misconfiguration — \
             check account funding, instrument symbols, and risk limits.",
            outcomes.rejected, outcomes.batch_ends,
        );
        std::process::exit(2);
    }
}

/// End-of-run pacing report. `None` when `--target-rate` is unset (the
/// closed-loop case); rendered into the JSON output and the console
/// summary lines otherwise.
pub(crate) struct PacingReport {
    pub target_rate: u64,
    pub scheduled: u64,
    pub late_sends: u64,
    pub max_send_delay_us: f64,
}

/// Print the outcome summary: acknowledged request count, rejection
/// ratio, and the top reject reasons. Surfaces misconfigured runs (e.g.
/// every order rejected with `InsufficientBalance`) that the latency
/// histogram would otherwise hide.
pub(crate) fn print_outcome_summary(outcomes: &OutcomeReport) {
    println!();
    println!("  Outcomes ({} acknowledged requests)", outcomes.batch_ends);
    if outcomes.batch_ends == 0 {
        println!("    (no responses observed — bench may have stalled before any ack)");
        return;
    }
    let total = outcomes.batch_ends as f64;
    let pct = |n: u64| n as f64 / total * 100.0;
    println!(
        "    rejected:  {:>10} ({:.2}%)",
        outcomes.rejected,
        pct(outcomes.rejected)
    );
    println!("    placed:    {:>10}", outcomes.placed);
    println!("    fills:     {:>10}", outcomes.fills);
    println!("    cancelled: {:>10}", outcomes.cancelled);
    if outcomes.triggered > 0 {
        println!("    triggered: {:>10}", outcomes.triggered);
    }
    if outcomes.replaced > 0 {
        println!("    replaced:  {:>10}", outcomes.replaced);
    }
    if outcomes.engine_errors > 0 {
        println!(
            "    engine errors: {} ({:.2}%)",
            outcomes.engine_errors,
            pct(outcomes.engine_errors)
        );
    }
    if outcomes.server_busy > 0 {
        println!(
            "    server-busy:   {} ({:.2}%)",
            outcomes.server_busy,
            pct(outcomes.server_busy)
        );
    }
    if outcomes.rejected > 0 {
        let mut reasons: Vec<(&str, u64)> = REJECT_REASONS
            .iter()
            .enumerate()
            .filter_map(|(i, (_, name))| {
                let count = outcomes.reject_reasons[i];
                if count > 0 {
                    Some((*name, count))
                } else {
                    None
                }
            })
            .collect();
        // Descending by count so the dominant reason is on top.
        reasons.sort_by_key(|r| std::cmp::Reverse(r.1));
        println!("    reject reasons:");
        for (name, count) in reasons.iter().take(5) {
            println!("      {name}: {count} ({:.2}%)", pct(*count));
        }
    }
}

/// Print benchmark results: header, throughput, latency histogram.
/// Optionally writes results to a JSON file for post-processing.
#[allow(clippy::too_many_arguments)]
pub(crate) fn print_results(
    label: &str,
    measured_orders: usize,
    phases: BenchPhases,
    histogram: &Histogram<u64>,
    wall: Duration,
    extra_lines: &[String],
    json_path: Option<&std::path::Path>,
    series: &[LatencySample],
    health: &health::HealthReport,
    server_stages: &stats::Body,
    pacing: Option<&PacingReport>,
    outcomes: Option<&OutcomeReport>,
) {
    let throughput = (measured_orders as f64) / wall.as_secs_f64();
    let wall_ms = wall.as_micros() as f64 / 1000.0;

    println!(
        "=== {label} Benchmark ({measured_orders} measured, warmup={} measured={} cooldown={}) ===",
        humantime::format_duration(phases.warmup),
        humantime::format_duration(phases.measured),
        humantime::format_duration(phases.cooldown),
    );
    for line in extra_lines {
        println!("{line}");
    }
    println!();
    println!("  Throughput");
    println!("    wall time:  {wall_ms:.2} ms");
    println!(
        "    throughput: {throughput:.0} orders/sec ({:.2} µs/order)",
        1_000_000.0 / throughput
    );
    println!();
    println!("  Per-Order Latency");
    print_latency_histogram(histogram, measured_orders);

    // Print outcome summary if we tracked responses.
    if let Some(outcomes) = outcomes {
        print_outcome_summary(outcomes);
    }

    // Print health summary if we collected anything — drops alone (no
    // successful sample) still warrant a line so the gap is visible.
    if !health.samples.is_empty() || health.dropped > 0 {
        let duration = health.samples.last().map_or(0.0, |s| s.elapsed_secs)
            - health.samples.first().map_or(0.0, |s| s.elapsed_secs);
        let peak_depth = health
            .samples
            .iter()
            .map(|s| s.input_queue_depth)
            .max()
            .unwrap_or(0);
        let capacity = health
            .samples
            .iter()
            .map(|s| s.input_queue_capacity)
            .max()
            .unwrap_or(0);
        let final_events = health.samples.last().map_or(0, |s| s.events_processed);
        println!();
        let total = health.samples.len() as u64 + health.dropped;
        let drop_pct = if total > 0 {
            health.dropped as f64 / total as f64 * 100.0
        } else {
            0.0
        };
        println!(
            "  Health ({} samples over {duration:.1}s, {dropped} dropped, {drop_pct:.1}%)",
            health.samples.len(),
            dropped = health.dropped,
        );
        if capacity > 0 {
            let pct = peak_depth as f64 / capacity as f64 * 100.0;
            println!("    peak queue depth: {peak_depth} / {capacity} ({pct:.1}%)");
        } else {
            println!("    peak queue depth: {peak_depth}");
        }
        println!("    events processed: {final_events}");
    }

    // Server-side per-stage decomposition (tick-to-trade). Fetched
    // from the server's /stats-dump endpoint at end of run; only
    // populated for the roundtrip mode against a server built with
    // --features latency-trace.
    stats::render_console(server_stages);

    // Write JSON results if requested.
    if let Some(path) = json_path {
        use std::io::Write;

        let throughput = (measured_orders as f64) / wall.as_secs_f64();
        let mut percentiles = String::from("{");
        percentiles.push_str(&format!(
            "\"min_us\":{:.2},\"p50_us\":{:.2},\"p90_us\":{:.2}",
            histogram.min() as f64 / 1000.0,
            histogram.value_at_quantile(0.50) as f64 / 1000.0,
            histogram.value_at_quantile(0.90) as f64 / 1000.0,
        ));
        let mut n = 2;
        let mut t = 1_000usize;
        while t <= measured_orders {
            let q = 1.0 - 10.0f64.powi(-(n as i32));
            let label = if n <= 2 {
                "p99_us".to_string()
            } else {
                format!("p99{}_us", ".9".repeat(n - 2))
            };
            percentiles.push_str(&format!(
                ",\"{}\":{:.2}",
                label,
                histogram.value_at_quantile(q) as f64 / 1000.0
            ));
            n += 1;
            t *= 10;
        }
        percentiles.push_str(&format!(
            ",\"max_us\":{:.2}}}",
            histogram.max() as f64 / 1000.0
        ));

        // Serialize time-series data for stability plots. Schema owned by
        // the harness so `melin-plot` has one definition to track.
        let ts_json = series::to_json(series);

        // Serialize health samples (fixed fields + any extra metrics).
        let health_json = if health.samples.is_empty() {
            String::from("[]")
        } else {
            let entries: Vec<String> = health.samples
                .iter()
                .map(|s| {
                    let mut json = format!(
                        "{{\"elapsed_secs\":{:.3},\"active_connections\":{},\"events_processed\":{},\"journal_sequence\":{},\"replication_lag\":{},\"input_queue_depth\":{},\"input_queue_capacity\":{},\"pipeline_healthy\":{},\"trading_active\":{}",
                        s.elapsed_secs,
                        s.active_connections,
                        s.events_processed,
                        s.journal_sequence,
                        s.replication_lag,
                        s.input_queue_depth,
                        s.input_queue_capacity,
                        s.pipeline_healthy,
                        s.trading_active,
                    );
                    // Append extra metrics (per-replica replication stats, etc.).
                    // Sorted for deterministic output. Prometheus label syntax
                    // like `metric{slot="0"}` is sanitized to `metric_slot_0`
                    // for valid JSON keys.
                    let mut keys: Vec<&String> = s.extra.keys().collect();
                    keys.sort();
                    for key in keys {
                        let val = s.extra[key];
                        // Sanitize Prometheus label syntax for JSON keys:
                        // melin_replica_lag{slot="0"} → melin_replica_lag_slot_0
                        let safe_key: String = key
                            .chars()
                            .filter_map(|c| match c {
                                '{' | '=' => Some('_'),
                                '}' | '"' => None,
                                other => Some(other),
                            })
                            .collect();
                        // Emit integers without decimal point for cleaner JSON.
                        if val == val.trunc() && val.abs() < u64::MAX as f64 {
                            json.push_str(&format!(",\"{safe_key}\":{}", val as i64));
                        } else {
                            json.push_str(&format!(",\"{safe_key}\":{val:.3}"));
                        }
                    }
                    json.push('}');
                    json
                })
                .collect();
            format!("[{}]", entries.join(","))
        };

        let stages_json = stats::render_json(server_stages);

        // Outcome fragment: emitted only when response tracking was on,
        // so the schema for in-process modes (engine, pipeline) that
        // don't observe wire responses is unchanged.
        let outcomes_json = match outcomes {
            Some(o) => {
                let mut reasons = String::from("{");
                let mut first = true;
                for (i, (_, name)) in REJECT_REASONS.iter().enumerate() {
                    let count = o.reject_reasons[i];
                    if count == 0 {
                        continue;
                    }
                    if !first {
                        reasons.push(',');
                    }
                    first = false;
                    reasons.push_str(&format!("\"{name}\":{count}"));
                }
                reasons.push('}');
                format!(
                    ",\"outcomes\":{{\"batch_ends\":{},\"placed\":{},\"fills\":{},\"cancelled\":{},\"triggered\":{},\"replaced\":{},\"rejected\":{},\"engine_errors\":{},\"server_busy\":{},\"reject_reasons\":{reasons}}}",
                    o.batch_ends,
                    o.placed,
                    o.fills,
                    o.cancelled,
                    o.triggered,
                    o.replaced,
                    o.rejected,
                    o.engine_errors,
                    o.server_busy,
                )
            }
            None => String::new(),
        };

        // Pacing fragment: emitted only when target-rate was set, so the
        // schema for closed-loop runs is unchanged.
        let pacing_json = match pacing {
            Some(p) => format!(
                ",\"pacing\":{{\"target_rate\":{},\"scheduled\":{},\"achieved_rate\":{:.0},\"late_sends\":{},\"max_send_delay_us\":{:.2}}}",
                p.target_rate, p.scheduled, throughput, p.late_sends, p.max_send_delay_us,
            ),
            None => String::new(),
        };

        let json = format!(
            "{{\"label\":\"{label}\",\"measured_orders\":{measured_orders},\"warmup_ms\":{:.2},\"measured_ms\":{:.2},\"cooldown_ms\":{:.2},\"wall_ms\":{:.2},\"throughput_ops\":{:.0},\"latency\":{percentiles},\"time_series\":{ts_json},\"health\":{health_json},\"server_stages\":{stages_json}{pacing_json}{outcomes_json}}}",
            phases.warmup.as_secs_f64() * 1000.0,
            phases.measured.as_secs_f64() * 1000.0,
            phases.cooldown.as_secs_f64() * 1000.0,
            wall.as_secs_f64() * 1000.0,
            throughput,
        );

        let mut file = std::fs::File::create(path).expect("create json file");
        file.write_all(json.as_bytes()).expect("write json");
        file.write_all(b"\n").expect("write newline");
        eprintln!("Results written to {}", path.display());
    }
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

#[cfg(test)]
mod outcome_report_tests {
    use super::*;

    fn dummy_order() -> (OrderId, Symbol, AccountId) {
        (OrderId(1), Symbol(0), AccountId(7))
    }

    fn one_qty() -> Quantity {
        Quantity(NonZeroU64::new(1).unwrap())
    }

    fn one_price() -> Price {
        Price(NonZeroU64::new(100).unwrap())
    }

    #[test]
    fn records_each_variant_into_the_right_bucket() {
        let (oid, sym, acc) = dummy_order();
        let mut r = OutcomeReport::default();
        r.record(&ResponseKind::BatchEnd);
        r.record(&ResponseKind::Report(ExecutionReport::Placed {
            order_id: oid,
            symbol: sym,
            account: acc,
            side: Side::Buy,
            price: one_price(),
            quantity: one_qty(),
        }));
        r.record(&ResponseKind::Report(ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::InsufficientBalance,
        }));
        r.record(&ResponseKind::EngineError);
        r.record(&ResponseKind::ServerBusy);
        // Heartbeat is intentionally untracked.
        r.record(&ResponseKind::Heartbeat);

        assert_eq!(r.batch_ends, 1);
        assert_eq!(r.placed, 1);
        assert_eq!(r.rejected, 1);
        assert_eq!(r.engine_errors, 1);
        assert_eq!(r.server_busy, 1);
        assert_eq!(
            r.reject_reasons[reject_reason_index(RejectReason::InsufficientBalance)],
            1
        );
    }

    #[test]
    fn merge_sums_all_fields_including_reason_buckets() {
        let (oid, sym, acc) = dummy_order();
        let mut a = OutcomeReport::default();
        a.record(&ResponseKind::Report(ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::NoLiquidity,
        }));
        a.record(&ResponseKind::BatchEnd);

        let mut b = OutcomeReport::default();
        b.record(&ResponseKind::Report(ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::NoLiquidity,
        }));
        b.record(&ResponseKind::BatchEnd);
        b.record(&ResponseKind::BatchEnd);

        a.merge(&b);
        assert_eq!(a.batch_ends, 3);
        assert_eq!(a.rejected, 2);
        assert_eq!(
            a.reject_reasons[reject_reason_index(RejectReason::NoLiquidity)],
            2
        );
    }

    #[test]
    fn rejection_ratio_is_zero_when_no_batches_observed() {
        let r = OutcomeReport::default();
        // A stalled run with zero responses returns 0.0 rather than NaN
        // or 1.0 — distinct failure mode, surfaced by throughput, not by
        // the threshold check.
        assert_eq!(r.rejection_ratio(), 0.0);
    }

    #[test]
    fn rejection_ratio_divides_rejected_by_batch_ends() {
        let r = OutcomeReport {
            batch_ends: 1000,
            rejected: 25,
            ..OutcomeReport::default()
        };
        assert!((r.rejection_ratio() - 0.025).abs() < 1e-9);
    }

    #[test]
    fn record_execution_report_and_record_agree_on_report_variants() {
        // Engine and pipeline modes call `record_execution_report`
        // directly; the network bench reaches it via `record` ->
        // `ResponseKind::Report(_)`. Both paths must produce identical
        // counter state for the same input.
        let (oid, sym, acc) = dummy_order();
        let rep = ExecutionReport::Rejected {
            order_id: oid,
            symbol: sym,
            account: acc,
            reason: RejectReason::ExceedsMaxOrderQty,
        };

        let mut via_direct = OutcomeReport::default();
        via_direct.record_execution_report(&rep);

        let mut via_wire = OutcomeReport::default();
        via_wire.record(&ResponseKind::Report(rep));

        assert_eq!(via_direct.rejected, via_wire.rejected);
        assert_eq!(via_direct.reject_reasons, via_wire.reject_reasons);
    }

    #[test]
    fn reject_reasons_indices_are_unique_and_match_table_length() {
        // Sanity check: REJECT_REASONS and reject_reason_index must stay
        // in lockstep. Indices must cover [0, len) without collisions.
        let mut seen = vec![false; REJECT_REASONS.len()];
        for (reason, _) in REJECT_REASONS {
            let idx = reject_reason_index(*reason);
            assert!(idx < REJECT_REASONS.len(), "index {idx} out of range");
            assert!(!seen[idx], "duplicate index {idx} for {reason:?}");
            seen[idx] = true;
        }
        assert!(seen.iter().all(|b| *b), "missing variant in REJECT_REASONS");
    }
}
