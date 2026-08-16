//! In-process replication-pipeline benchmark.
//!
//! Drives the production `run_sender` (`tcp_sender.rs`) and
//! `run_receiver` (`tcp_receiver.rs`) code paths over kernel
//! localhost TCP, with a synthetic event generator feeding the
//! primary's input ring and a no-op consumer draining the
//! primary's output ring. Measures the throughput of the full
//! replication path:
//!
//!   generator → input ring → journal stage → replication ring →
//!   run_sender → kernel TCP localhost → run_receiver → replica
//!   input ring → replica journal + matching + drain → ack →
//!   replica slot cursor advance.
//!
//! Runs `--replicas` receivers (default 2, the production topology cap)
//! against one primary, so the reported figure is quorum throughput —
//! paced by the *slowest* replica, which is what the durability gate
//! actually waits on. The spread against the fastest replica's ack is
//! reported alongside it. `--replicas 1` collapses the quorum to a single
//! slot, matching what this bench measured before per-replica slots.
//!
//! Built with the `skip-order-exec` feature so the matching stage
//! short-circuits on both sides — what we measure is the replication
//! plumbing, not exchange logic. Built with `no-persist` to skip
//! disk I/O so the replication path's CPU cost dominates.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use base64::Engine as _;
use clap::Parser;
use ed25519_dalek::SigningKey;

use melin_app::auth::AuthorizedKeys;
use melin_app::unix_epoch_nanos;
use melin_journal::JournalEvent;
#[allow(unused_imports)] // used by some feature combinations only
use melin_journal::JournalWrite;
use melin_server::exchange_app::ServerApp;
use melin_server_runtime::durability_policy::DurabilityMode;
use melin_server_runtime::replication::{
    ReplicaControlPlane, ReplicationListener, ReplicationMetrics, Sender, run_receiver, run_sender,
};
use melin_server_runtime::server::PipelineCores;
use melin_trading::trading_event::TradingEvent;
type InputSlot = melin_transport_core::pipeline::InputSlot<TradingEvent>;
type OutputSlot = melin_transport_core::pipeline::OutputSlot<
    melin_types::types::ExecutionReport,
    melin_types::types::QueryResponse,
>;
use melin_transport_core::JournaledApp;
use melin_transport_core::pipeline::{JournalStageRun, build_pipeline_with_replication};
use melin_transport_core::trace::mono_trace_ns;
use melin_types::types::{AccountId, CurrencyId};

#[derive(Parser)]
struct Args {
    /// Yield instead of busy-spinning when pipeline stages are idle.
    /// On machines without isolated CPUs, this frees cores for the journal
    /// and sender stages. Use to compare throughput vs the default
    /// busy-spin mode.
    #[arg(long)]
    no_busy_spin: bool,

    /// Number of replicas to run in-process. Capped at
    /// `ReplicaSlotCursors::SLOTS` — the sender is provisioned for exactly
    /// the `1 primary + 2 replicas` production topology. Default 2 measures
    /// what durability actually gates on (the *slowest* replica's ack);
    /// `--replicas 1` degenerates the quorum to a single slot, which is the
    /// shape this bench measured before per-replica slots existed.
    #[arg(long, default_value_t = melin_transport_core::ReplicaSlotCursors::SLOTS)]
    replicas: usize,

    /// Durability mode advertised to replicas on `StreamStart` and every
    /// heartbeat. This bench drains the output ring with a no-op instead of
    /// running the response gate, so the mode does not throttle the
    /// generator — it only sets what replicas judge auto-promotion against.
    #[arg(long, default_value = "hybrid")]
    durability: DurabilityArg,
}

/// Mirrors [`DurabilityMode`] as a clap-parsable value. The runtime enum
/// isn't `ValueEnum`, and deriving it here keeps the dependency one-way.
#[derive(Clone, Copy, clap::ValueEnum)]
enum DurabilityArg {
    Local,
    Hybrid,
    DurablyReplicated,
}

impl From<DurabilityArg> for DurabilityMode {
    fn from(arg: DurabilityArg) -> Self {
        match arg {
            DurabilityArg::Local => DurabilityMode::Local,
            DurabilityArg::Hybrid => DurabilityMode::Hybrid,
            DurabilityArg::DurablyReplicated => DurabilityMode::DurablyReplicated,
        }
    }
}

const PRIMARY_REPL_ADDR: &str = "127.0.0.1:39877";
const RUN_SECS: u64 = 10;
const MAX_JOURNAL_BATCH: usize = 4096;
/// Ring depth in batches. Production default is 256 but this bench's
/// generator outruns the replica enough to trigger eviction in the
/// first second of a 256-deep ring; bumping to 4096 gives a clean
/// steady-state window. Power of two required by the SPSC ring.
const REPLICATION_RING_SIZE: usize = 4096;
const BATCH_SIZE: usize = 32;
const HEARTBEAT_SECS: u64 = 5;

fn main() {
    let args = Args::parse();
    let busy_spin = !args.no_busy_spin;
    let durability: DurabilityMode = args.durability.into();

    let n_replicas = args.replicas;
    if n_replicas == 0 || n_replicas > melin_transport_core::ReplicaSlotCursors::SLOTS {
        eprintln!(
            "FATAL: --replicas must be 1..={}",
            melin_transport_core::ReplicaSlotCursors::SLOTS
        );
        std::process::exit(2);
    }

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    eprintln!("replication-bench: setting up (busy_spin={})", busy_spin);

    // --- Auth keys ---
    // Each replica signs its handshake with its own key; the primary's
    // authorized_keys lists every replica's public key. Distinct keys per
    // replica rather than one shared key: that is how a real deployment is
    // provisioned, and it keeps the two handshakes independently
    // attributable in the sender's logs. Deterministic seeds — this is a
    // self-contained bench, not a security-sensitive context.
    //
    // Vec: length is a runtime `--replicas` value, so this cannot be a
    // fixed-size array without threading a const generic through.
    let replica_keys: Vec<SigningKey> = (0..n_replicas)
        .map(|i| SigningKey::from_bytes(&[0x42u8 + i as u8; 32]))
        .collect();
    let mut auth_text = String::new();
    for (i, key) in replica_keys.iter().enumerate() {
        let pub_b64 =
            base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
        auth_text.push_str(&format!("replication {pub_b64} bench-replica-{i}\n"));
    }
    let authorized_keys =
        Arc::new(AuthorizedKeys::parse(&auth_text).expect("parse authorized_keys"));

    // --- Tempdir for journal files ---
    let tmp_root: PathBuf =
        std::env::temp_dir().join(format!("melin-replication-bench-{}", std::process::id()));
    std::fs::create_dir_all(&tmp_root).expect("mkdir tempdir");
    let primary_journal: PathBuf = tmp_root.join("primary.journal");

    // --- Build primary pipeline ---
    // Bench runs the buffered writer end-to-end; the sector path is
    // exercised separately in pipeline tests until the boot-site
    // dispatch refactor lands.
    let engine = JournaledApp::<ServerApp, melin_journal::BufferedWriter<_>>::create(
        ServerApp(melin_exchange_core::exchange::Exchange::with_capacity()),
        &primary_journal,
    )
    .expect("create primary journal");
    let (exchange, writer) = engine.into_parts();

    let active_connections = Arc::new(AtomicU64::new(0));
    let primary_fence = Arc::new(melin_transport_core::fence::FenceState::new(0));

    let pipeline = build_pipeline_with_replication(
        exchange,
        writer,
        Duration::ZERO,
        Arc::clone(&active_connections),
        true, // enable_replication
        MAX_JOURNAL_BATCH,
        REPLICATION_RING_SIZE,
        busy_spin,
        false, // enable_event_publisher
        false, // enable_shadow
        Arc::clone(&primary_fence),
    );

    let mut input_producer = pipeline.input_producer;
    let journal_stage = pipeline.journal_stage;
    let matching_stage = pipeline.matching_stage;
    let mut output_consumers = pipeline.output_consumers;
    // Per-replica ack slots. `quorum_acked()` is the min over engaged
    // slots (what durability gates on) and `fastest_acked()` the max; the
    // spread between them is the lag the slowest replica imposes.
    let replica_slots = pipeline.cursors.replica_slot_cursors();
    let (repl_consumer_1, repl_consumer_2) =
        pipeline.replication_consumers.expect("replication enabled");
    let replication_ring_progress = pipeline
        .replication_ring_progress
        .expect("replication enabled");

    // Pop consumer 0 — the production response stage drains it. We
    // don't run the response stage (it's irrelevant to replication
    // throughput); spawn a no-op drain thread instead.
    let output_consumer_0 = output_consumers.remove(0);

    let shutdown = Arc::new(AtomicBool::new(false));

    // --- Spawn primary pipeline stages ---
    let s = Arc::clone(&shutdown);
    let journal_handle = std::thread::Builder::new()
        .name("bench-journal".into())
        .spawn(move || {
            let _ = journal_stage.run(&s);
        })
        .expect("spawn journal");

    let s = Arc::clone(&shutdown);
    let matching_handle = std::thread::Builder::new()
        .name("bench-matching".into())
        .spawn(move || matching_stage.run(&s))
        .expect("spawn matching");

    // No-op drain of the output ring (replaces production response stage).
    let s = Arc::clone(&shutdown);
    let drain_handle = std::thread::Builder::new()
        .name("bench-drain".into())
        .spawn(move || {
            let mut consumer = output_consumer_0;
            let mut batch = vec![OutputSlot::default(); 256];
            loop {
                if s.load(Ordering::Relaxed) {
                    return;
                }
                let n = consumer.consume_batch(&mut batch, 256);
                if n == 0 {
                    if busy_spin {
                        std::hint::spin_loop();
                    } else {
                        std::thread::yield_now();
                    }
                }
            }
        })
        .expect("spawn drain");

    // --- Spawn run_sender ---
    // 0.13 binds the replication listener at the call site rather than on
    // the sender thread, so a port conflict fails here with a clear error
    // instead of silently killing the sender.
    let bind_addr: std::net::SocketAddr = PRIMARY_REPL_ADDR.parse().expect("parse repl addr");
    let listener = ReplicationListener::new(
        std::net::TcpListener::bind(bind_addr).expect("bind replication listener"),
    )
    .expect("set replication listener non-blocking");
    let metrics = Arc::new(ReplicationMetrics::default());
    let ready_flag = Arc::new(AtomicBool::new(false));
    let connected_counter = Arc::new(AtomicU32::new(0));
    let durability_mode = Arc::new(std::sync::atomic::AtomicU8::new(durability.as_u8()));

    let sender_config = Sender {
        listener,
        repl_consumer_1,
        repl_consumer_2,
        replica_slots: Arc::clone(&replica_slots),
        durability_mode: Arc::clone(&durability_mode),
        journal_path: primary_journal.clone(),
        authorized_keys: Arc::clone(&authorized_keys),
        evict_flags: replication_ring_progress.evict_flags.clone(),
        active_flags: replication_ring_progress.active_flags.clone(),
        metrics: Arc::clone(&metrics),
        handler_cores: [0, 0], // 0 = unpinned
        batch_size: BATCH_SIZE,
        heartbeat_secs: HEARTBEAT_SECS,
        busy_spin,
        fence_state: Arc::clone(&primary_fence),
    };

    let s = Arc::clone(&shutdown);
    let r = Arc::clone(&ready_flag);
    let c = Arc::clone(&connected_counter);
    let sender_handle = std::thread::Builder::new()
        .name("bench-repl-sender".into())
        .spawn(move || run_sender::<ServerApp>(sender_config, &s, &r, &c))
        .expect("spawn run_sender");

    // --- Spawn run_receiver, one per replica ---
    // Each receiver is self-contained: builds its own replica pipeline
    // (input ring + journal + matching + drain + shadow) internally and
    // drives it from the wire stream. They connect to the same primary
    // address; the sender's two handler threads land them in slots 0 and 1.
    //
    // Everything below is per-replica — journal, snapshot, signing key,
    // fence epoch and control plane are all distinct. Sharing any of them
    // would have the two replicas fighting over one journal directory.
    //
    // Vec: count is the runtime `--replicas` value; see `replica_keys`.
    let mut receiver_handles = Vec::with_capacity(n_replicas);
    for (i, replica_key) in replica_keys.into_iter().enumerate() {
        let cores = PipelineCores {
            journal: 0,
            matching: 0,
            response: 0,
            reader: 0,
            repl_sender: 0,
            event_publisher: 0,
            shadow: 0,
            repl_handler_0: 0,
            repl_handler_1: 0,
            journal_prep: 0,
        };
        let replica_journal: PathBuf = tmp_root.join(format!("replica-{i}.journal"));
        let replica_snapshot: PathBuf = tmp_root.join(format!("replica-{i}.snapshot"));
        let s = Arc::clone(&shutdown);
        // Fresh handles: nothing promoted, tip not yet trustworthy, link
        // down until the handshake completes. The bench never files a
        // promotion — it measures steady-state streaming, not failover.
        let control = ReplicaControlPlane::new();
        let replica_fence = Arc::new(melin_transport_core::fence::FenceState::new(0));
        let handle = std::thread::Builder::new()
            .name(format!("bench-repl-receiver-{i}"))
            .spawn(move || {
                let _ = run_receiver::<ServerApp, melin_journal::BufferedWriter<_>>(
                    bind_addr,
                    &replica_journal,
                    &replica_key,
                    &s,
                    &control,
                    3_000_000, // snapshot_interval_ms (effectively never)
                    replica_snapshot,
                    cores,
                    std::time::Duration::ZERO,
                    8, // pipeline_depth
                    busy_spin,
                    std::sync::Arc::new(melin_server::app_factory::Factory::new(
                        melin_server::app_factory::FactoryConfig {
                            accounts: 0,
                            instruments: 0,
                            max_orders_per_account: 10_000,
                            max_orders_per_second: 0,
                            max_orders_burst: 0,
                        },
                    )),
                    replica_fence,
                );
            })
            .expect("spawn run_receiver");
        receiver_handles.push(handle);
    }

    // Wait for every replica to connect — quorum is only meaningful once
    // all slots are engaged, and starting the generator early would credit
    // the warm-up to a smaller quorum.
    let connect_deadline = Instant::now() + Duration::from_secs(10);
    while (connected_counter.load(Ordering::Acquire) as usize) < n_replicas {
        if Instant::now() > connect_deadline {
            eprintln!(
                "FATAL: only {}/{n_replicas} replicas connected within 10s",
                connected_counter.load(Ordering::Acquire)
            );
            shutdown.store(true, Ordering::Release);
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    eprintln!("{n_replicas} replica(s) connected, durability={durability:?}");

    // Seed: register one account so subsequent Deposit events
    // succeed under any future App that validates them.
    input_producer.publish(InputSlot {
        connection_id: 0,
        key_hash: 0,
        request_seq: 0,
        sequence: 0,
        timestamp_ns: unix_epoch_nanos(),
        event: JournalEvent::App(TradingEvent::ProvisionAccount {
            account: AccountId(1),
            amount: u64::MAX / 2,
        }),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });

    // --- Generator + stats reporter ---
    eprintln!("generator running for {RUN_SECS}s...");

    // Quorum ack as a plain count. `quorum_acked()` is `None` only while
    // *no* slot is engaged; with every replica connected it is the slowest
    // replica's ack.
    let quorum = || replica_slots.quorum_acked().map(|s| s.get()).unwrap_or(0);
    let fastest = || replica_slots.fastest_acked().map(|s| s.get()).unwrap_or(0);

    let bench_start = Instant::now();
    let deadline = bench_start + Duration::from_secs(RUN_SECS);
    let mut prev_repl_cursor = quorum();
    let mut prev_t = bench_start;
    let mut total_published: u64 = 0;
    let report_every = Duration::from_secs(1);
    let mut next_report = bench_start + report_every;

    'outer: while Instant::now() < deadline {
        // Tight publish loop — the generator's only job is to keep
        // the input ring full so downstream stages can run at
        // their own pace. `publish` spins on backpressure when
        // the ring is full.
        //
        // Pace by the replication_cursor: don't outrun it by more
        // than `lead_cap` events, otherwise the replication ring
        // fills, the journal stage evicts the replica, and the
        // bench wedges. The replica's drain rate is the steady-
        // state ceiling; pacing keeps us at that ceiling.
        let lead_cap = (BATCH_SIZE * REPLICATION_RING_SIZE / 2) as u64;
        let cur = quorum();
        // Watch the connection count, not the quorum cursor: `quorum_acked`
        // is a min over *engaged* slots, so a replica dropping mid-run
        // silently degrades the quorum to the survivors rather than
        // surfacing a sentinel. Only an all-slots-disengaged run reads as
        // `None`, which would be indistinguishable from a slow start.
        if (connected_counter.load(Ordering::Acquire) as usize) < n_replicas {
            eprintln!("WARN: a replica disconnected mid-run — stopping");
            break 'outer;
        }
        if total_published > cur + lead_cap {
            // Brief sleep, not a busy spin, to yield to the
            // pipeline stages.
            std::thread::sleep(Duration::from_micros(50));
        } else {
            for _ in 0..1024 {
                input_producer.publish(InputSlot {
                    connection_id: 0,
                    key_hash: 0,
                    request_seq: 0,
                    sequence: 0,
                    timestamp_ns: unix_epoch_nanos(),
                    event: JournalEvent::App(TradingEvent::Deposit {
                        account: AccountId(1),
                        currency: CurrencyId(1),
                        amount: 1,
                    }),
                    publish_ts: mono_trace_ns(),
                    recv_ts: mono_trace_ns(),
                });
                total_published += 1;
            }
        }

        let now = Instant::now();
        if now >= next_report {
            let cur = quorum();
            let lead = fastest();
            let dt = (now - prev_t).as_secs_f64();
            let dseq = cur.saturating_sub(prev_repl_cursor);
            eprintln!(
                "  [{:>5.1}s] published {:>10} quorum {:>10} delta {:>9} ({:>7.0} ev/s) spread {:>8}",
                bench_start.elapsed().as_secs_f64(),
                total_published,
                cur,
                dseq,
                dseq as f64 / dt,
                lead.saturating_sub(cur),
            );
            prev_repl_cursor = cur;
            prev_t = now;
            next_report = now + report_every;
        }
    }

    // --- Final report ---
    let total_wall = bench_start.elapsed().as_secs_f64();
    let final_cur = quorum();
    let final_lead = fastest();
    eprintln!();
    eprintln!("final ({total_wall:.2}s wall, {n_replicas} replica(s), durability={durability:?}):");
    eprintln!("  total events published:  {total_published}");
    eprintln!("  quorum acked:            {final_cur}");
    eprintln!("  fastest replica acked:   {final_lead}");
    eprintln!(
        "  slowest-replica lag:     {}",
        final_lead.saturating_sub(final_cur)
    );
    eprintln!(
        "  sustained throughput:    {:.0} ev/s",
        final_cur as f64 / total_wall
    );

    // --- Shutdown ---
    shutdown.store(true, Ordering::Release);
    let _ = journal_handle.join();
    let _ = matching_handle.join();
    let _ = drain_handle.join();
    let _ = sender_handle.join();
    for handle in receiver_handles {
        let _ = handle.join();
    }
}
