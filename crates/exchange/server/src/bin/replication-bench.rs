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

    /// Primary-side core assignment, seven comma-separated IDs in the
    /// order `generator,journal,matching,drain,repl-sender,handler-0,handler-1`.
    /// 0 leaves an entry unpinned, and omitting the flag leaves every
    /// primary thread unpinned — which is only sane on a host without
    /// isolated cores. Under `isolcpus` the scheduler will not migrate a
    /// thread onto or between isolated cores, so unpinned threads all
    /// inherit whichever core the process started on and the whole bench
    /// measures one core's worth of contention. Suggested on a 16-core
    /// host: `--cores 1,2,3,4,5,6,7`.
    #[arg(long, value_delimiter = ',')]
    cores: Option<Vec<usize>>,

    /// First core of each replica's own pipeline, one entry per replica.
    /// Replica `i` takes `base..=base+3` for its journal, matching, drain
    /// and receiver threads; its shadow stage stays unpinned because this
    /// bench never snapshots. 0 leaves that replica unpinned. Suggested
    /// alongside the `--cores` example above: `--replica-cores 8,12`.
    #[arg(long, value_delimiter = ',')]
    replica_cores: Option<Vec<usize>>,
}

/// Threads a replica pins out of its `--replica-cores` base.
const CORES_PER_REPLICA: usize = 4;

/// Entries `--cores` expects: generator, journal, matching, drain,
/// repl-sender, handler-0, handler-1.
const PRIMARY_CORE_SLOTS: usize = 7;

/// Primary-side core assignment, resolved from `--cores`. All-zero (every
/// thread unpinned) is the default, preserving the behavior of runs from
/// before the flag existed.
#[derive(Debug, Clone, Copy, Default)]
struct PrimaryCores {
    generator: usize,
    journal: usize,
    matching: usize,
    drain: usize,
    repl_sender: usize,
    handler_0: usize,
    handler_1: usize,
}

/// Pin the calling thread to `core`, or leave it where it is when `core`
/// is 0 — the same "unpinned" sentinel `PipelineCores` uses. A failure to
/// pin is a warning, not a fatal: the run still produces a number, it is
/// just a number with a caveat, and the startup banner records what was
/// asked for.
fn pin(label: &str, core: usize) {
    if core == 0 {
        return;
    }
    if let Err(e) = melin_app::affinity::pin_to_core(core) {
        eprintln!("warning: could not pin {label} to core {core}: {e}");
    }
}

/// Resolve `--cores` / `--replica-cores` into a primary assignment plus one
/// base core per replica, rejecting anything that would put two spinning
/// threads on one core. That collision does not announce itself — the
/// threads are SCHED_FIFO once pinned, so one spins and the other starves,
/// and the run reports a plausible-looking but meaningless number.
fn resolve_cores(
    cores: Option<&Vec<usize>>,
    replica_cores: Option<&Vec<usize>>,
    n_replicas: usize,
) -> Result<(PrimaryCores, Vec<usize>), String> {
    let primary = match cores {
        None => PrimaryCores::default(),
        Some(v) if v.len() == PRIMARY_CORE_SLOTS => PrimaryCores {
            generator: v[0],
            journal: v[1],
            matching: v[2],
            drain: v[3],
            repl_sender: v[4],
            handler_0: v[5],
            handler_1: v[6],
        },
        Some(v) => {
            return Err(format!(
                "--cores expects {PRIMARY_CORE_SLOTS} comma-separated IDs \
                 (generator,journal,matching,drain,repl-sender,handler-0,handler-1), got {}",
                v.len()
            ));
        }
    };

    let bases = match replica_cores {
        None => vec![0; n_replicas],
        Some(v) if v.len() == n_replicas => v.clone(),
        Some(v) => {
            return Err(format!(
                "--replica-cores expects one base core per replica ({n_replicas}), got {}",
                v.len()
            ));
        }
    };

    // (core, owner) for every pinned thread, so a duplicate can name both
    // sides of the clash rather than just the number.
    let mut claimed: Vec<(usize, String)> = vec![
        (primary.generator, "generator".to_string()),
        (primary.journal, "journal".to_string()),
        (primary.matching, "matching".to_string()),
        (primary.drain, "drain".to_string()),
        (primary.repl_sender, "repl-sender".to_string()),
        (primary.handler_0, "handler-0".to_string()),
        (primary.handler_1, "handler-1".to_string()),
    ];
    for (i, base) in bases.iter().enumerate() {
        if *base == 0 {
            continue;
        }
        for (offset, role) in ["journal", "matching", "drain", "receiver"]
            .iter()
            .enumerate()
        {
            claimed.push((base + offset, format!("replica-{i} {role}")));
        }
    }
    claimed.retain(|(core, _)| *core != 0);
    for i in 0..claimed.len() {
        for j in (i + 1)..claimed.len() {
            if claimed[i].0 == claimed[j].0 {
                return Err(format!(
                    "core {} claimed by both {} and {} — two pinned spinners on one core \
                     starve each other",
                    claimed[i].0, claimed[i].1, claimed[j].1
                ));
            }
        }
    }

    Ok((primary, bases))
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

    let (primary_cores, replica_bases) =
        match resolve_cores(args.cores.as_ref(), args.replica_cores.as_ref(), n_replicas) {
            Ok(resolved) => resolved,
            Err(e) => {
                eprintln!("FATAL: {e}");
                std::process::exit(2);
            }
        };

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();

    eprintln!("replication-bench: setting up (busy_spin={})", busy_spin);
    if args.cores.is_none() && args.replica_cores.is_none() {
        eprintln!(
            "warning: no --cores/--replica-cores; every thread is unpinned. On a host with \
             isolated cores they will all share the one core this process started on, and the \
             figure below is that core's contention rather than replication throughput."
        );
    } else {
        eprintln!(
            "  primary: generator={} journal={} matching={} drain={} repl-sender={} \
             handlers={},{}",
            primary_cores.generator,
            primary_cores.journal,
            primary_cores.matching,
            primary_cores.drain,
            primary_cores.repl_sender,
            primary_cores.handler_0,
            primary_cores.handler_1,
        );
        for (i, base) in replica_bases.iter().enumerate() {
            if *base == 0 {
                eprintln!("  replica-{i}: unpinned");
            } else {
                eprintln!(
                    "  replica-{i}: cores {}-{}",
                    base,
                    base + CORES_PER_REPLICA - 1
                );
            }
        }
    }

    // The generator runs on this thread — pin it before it starts
    // publishing, not after.
    pin("generator", primary_cores.generator);

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
    let journal_core = primary_cores.journal;
    let journal_handle = std::thread::Builder::new()
        .name("bench-journal".into())
        .spawn(move || {
            pin("journal", journal_core);
            let _ = journal_stage.run(&s);
        })
        .expect("spawn journal");

    let s = Arc::clone(&shutdown);
    let matching_core = primary_cores.matching;
    let matching_handle = std::thread::Builder::new()
        .name("bench-matching".into())
        .spawn(move || {
            pin("matching", matching_core);
            matching_stage.run(&s)
        })
        .expect("spawn matching");

    // No-op drain of the output ring (replaces production response stage).
    let s = Arc::clone(&shutdown);
    let drain_core = primary_cores.drain;
    let drain_handle = std::thread::Builder::new()
        .name("bench-drain".into())
        .spawn(move || {
            pin("drain", drain_core);
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
        handler_cores: [primary_cores.handler_0, primary_cores.handler_1],
        batch_size: BATCH_SIZE,
        heartbeat_secs: HEARTBEAT_SECS,
        busy_spin,
        fence_state: Arc::clone(&primary_fence),
    };

    let s = Arc::clone(&shutdown);
    let r = Arc::clone(&ready_flag);
    let c = Arc::clone(&connected_counter);
    let sender_core = primary_cores.repl_sender;
    let sender_handle = std::thread::Builder::new()
        .name("bench-repl-sender".into())
        .spawn(move || {
            pin("repl-sender", sender_core);
            run_sender::<ServerApp>(sender_config, &s, &r, &c)
        })
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
        // `base + 0..3` — journal, matching, drain, receiver — matching the
        // order documented on `--replica-cores`. A base of 0 leaves the
        // whole replica unpinned, since 0 is the unpinned sentinel and
        // `0 + offset` would otherwise claim cores 1-3.
        let base = replica_bases[i];
        let replica_core = |offset: usize| if base == 0 { 0 } else { base + offset };
        let cores = PipelineCores {
            journal: replica_core(0),
            matching: replica_core(1),
            response: replica_core(2),
            reader: replica_core(3),
            repl_sender: 0,
            event_publisher: 0,
            // Unpinned: this bench sets a snapshot interval of ~35 days, so
            // the shadow stage never does any work worth a core.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_flags_leave_everything_unpinned() {
        let (primary, bases) = resolve_cores(None, None, 2).unwrap();
        assert_eq!(primary.generator, 0);
        assert_eq!(primary.handler_1, 0);
        assert_eq!(bases, vec![0, 0]);
    }

    #[test]
    fn primary_cores_map_in_documented_order() {
        let cores = vec![1, 2, 3, 4, 5, 6, 7];
        let (primary, _) = resolve_cores(Some(&cores), None, 2).unwrap();
        assert_eq!(primary.generator, 1);
        assert_eq!(primary.journal, 2);
        assert_eq!(primary.matching, 3);
        assert_eq!(primary.drain, 4);
        assert_eq!(primary.repl_sender, 5);
        assert_eq!(primary.handler_0, 6);
        assert_eq!(primary.handler_1, 7);
    }

    #[test]
    fn wrong_primary_core_count_is_rejected() {
        let cores = vec![1, 2, 3];
        let err = resolve_cores(Some(&cores), None, 2).unwrap_err();
        assert!(err.contains("--cores expects 7"), "{err}");
    }

    #[test]
    fn replica_core_count_must_match_replica_count() {
        let bases = vec![8];
        let err = resolve_cores(None, Some(&bases), 2).unwrap_err();
        assert!(err.contains("one base core per replica (2)"), "{err}");
    }

    #[test]
    fn duplicate_primary_cores_are_rejected() {
        let cores = vec![1, 2, 2, 4, 5, 6, 7];
        let err = resolve_cores(Some(&cores), None, 2).unwrap_err();
        assert!(err.contains("core 2"), "{err}");
        assert!(err.contains("journal") && err.contains("matching"), "{err}");
    }

    /// The overlap that actually bites: a replica's four-core span running
    /// into a primary thread's core.
    #[test]
    fn replica_span_overlapping_the_primary_is_rejected() {
        let cores = vec![1, 2, 3, 4, 5, 6, 7];
        let bases = vec![5, 12];
        let err = resolve_cores(Some(&cores), Some(&bases), 2).unwrap_err();
        assert!(err.contains("core 5"), "{err}");
        assert!(err.contains("repl-sender"), "{err}");
        assert!(err.contains("replica-0"), "{err}");
    }

    #[test]
    fn replica_spans_overlapping_each_other_are_rejected() {
        let bases = vec![8, 10];
        let err = resolve_cores(None, Some(&bases), 2).unwrap_err();
        assert!(err.contains("core 10") || err.contains("core 11"), "{err}");
    }

    #[test]
    fn adjacent_replica_spans_are_accepted() {
        let cores = vec![1, 2, 3, 4, 5, 6, 7];
        let bases = vec![8, 12];
        let (_, resolved) = resolve_cores(Some(&cores), Some(&bases), 2).unwrap();
        assert_eq!(resolved, vec![8, 12]);
    }

    /// A zero base means "leave this replica alone" — it must not be read
    /// as claiming cores 0-3, which would collide with the primary.
    #[test]
    fn zero_replica_base_claims_nothing() {
        let cores = vec![1, 2, 3, 4, 5, 6, 7];
        let bases = vec![0, 12];
        let (_, resolved) = resolve_cores(Some(&cores), Some(&bases), 2).unwrap();
        assert_eq!(resolved, vec![0, 12]);
    }
}
