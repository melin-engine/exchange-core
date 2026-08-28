//! io_uring event loop driving all connections owned by one bench thread.
//!
//! Single-threaded per instance: RECV for reads, SEND for writes, one ring
//! per thread. What flows over those sockets is entirely up to the
//! [`Workload`] — this module owns only the framing (`[u32 LE len][body]`,
//! matching the sequencer's wire protocol), the send window, the pacing,
//! and the phase classification.

use std::collections::VecDeque;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use hdrhistogram::Histogram;
use io_uring::{IoUring, opcode, types};

use crate::clock::{calibrate_tsc, rdtscp, tsc_to_ns};
use crate::pacing::{PaceClock, PaceStats};
use crate::phases::{BenchPhases, PhaseDeadlines};
use crate::series::{TimeSeries, maybe_sample};
use crate::workload::{Outcomes, Workload};

/// Per-connection recv buffer size for io_uring RECV.
const RECV_BUF_SIZE: usize = 4096;

/// Maximum frame payload size. Matches the sequencer wire protocol's own
/// cap, and sizes each connection's parse buffer so a frame that arrives
/// split across two RECVs reassembles without reallocating.
const MAX_FRAME_SIZE: usize = 1024;

/// Flag bit in io_uring user_data to distinguish SEND from RECV CQEs.
/// Bit 63 set = SEND completion, clear = RECV completion.
const SEND_FLAG: u64 = 1 << 63;

/// Histogram bounds shared by the cumulative and interval histograms:
/// 1 ns to 10 s at 3 significant figures.
fn new_histogram() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds")
}

/// One benchmark connection: a socket pair (read half, write half), the
/// workload driving it, and the send/recv state the event loop keeps
/// between iterations.
pub struct Connection<W: Workload> {
    read_fd: RawFd,
    write_fd: RawFd,
    /// Owns the read half — keeps the fd alive.
    _read_owner: Box<dyn Send>,
    /// Owns the write half — keeps the fd alive.
    _write_owner: Box<dyn Send>,

    // Recv state
    recv_buf: Box<[u8; RECV_BUF_SIZE]>,
    parse_buf: Vec<u8>,
    recv_pending: bool,

    // Send state
    send_buf: Vec<u8>,
    send_pending: bool,

    /// Generates requests and classifies responses for this connection.
    workload: W,

    /// TSC tick at send time, one per in-flight request. `u64` instead of
    /// `Instant` to avoid ~15-25ns vDSO overhead per timestamp on the hot
    /// path. With open-loop pacing enabled this stores the *scheduled*
    /// TSC instead of the actual submission TSC — the standard
    /// coordinated-omission fix.
    inflight_ts: VecDeque<u64>,

    /// Open-loop scheduler (when `target_rate > 0`). Materialised inside
    /// [`run_loop`] where TSC calibration runs; `None` until then.
    pacer: Option<PaceClock>,
}

impl<W: Workload> Connection<W> {
    /// Build a connection from an already-connected, already-authenticated
    /// socket pair. The two halves are kept as separate owners because the
    /// kernel-TCP path splits a stream into independently-owned halves;
    /// pass the same handle twice for transports that don't.
    ///
    /// `window` sizes the in-flight queue up front so the hot path never
    /// grows it.
    pub fn new<R, S>(read_half: R, write_half: S, workload: W, window: usize) -> Self
    where
        R: AsRawFd + Send + 'static,
        S: AsRawFd + Send + 'static,
    {
        let read_fd = read_half.as_raw_fd();
        let write_fd = write_half.as_raw_fd();
        Self {
            read_fd,
            write_fd,
            _read_owner: Box::new(read_half),
            _write_owner: Box::new(write_half),
            recv_buf: Box::new([0u8; RECV_BUF_SIZE]),
            parse_buf: Vec::with_capacity(MAX_FRAME_SIZE + 4),
            recv_pending: false,
            send_buf: Vec::with_capacity(4096),
            send_pending: false,
            workload,
            inflight_ts: VecDeque::with_capacity(window),
            pacer: None,
        }
    }
}

/// Everything one bench thread's event loop needs beyond its connections.
pub struct LoopConfig {
    /// Maximum requests in flight per connection.
    pub window: usize,
    /// Shared run-start instant. All threads derive phase deadlines from
    /// the same instant so they classify completions consistently.
    pub bench_start: Instant,
    /// Phase cutoffs derived from `bench_start`.
    pub deadlines: PhaseDeadlines,
    /// Phase durations, used to gate pacing telemetry on warmup end.
    pub phases: BenchPhases,
    /// Run-wide completed-request counter feeding the progress reporter.
    pub progress: Arc<AtomicU64>,
    /// Aggregate open-loop target rate across *all* connections in the
    /// run. Zero means closed-loop (send as fast as the window allows).
    pub target_rate: u64,
    /// Connection count across the whole run, used to split `target_rate`.
    pub total_clients: usize,
    /// This thread's index, and the thread count. Together they map a
    /// local connection to its global index so the pacer stagger spreads
    /// first sends across every connection rather than within a thread.
    pub thread_idx: usize,
    pub total_threads: usize,
    /// Run-wide pacing telemetry.
    pub pace_stats: Arc<PaceStats>,
}

/// What one bench thread's event loop produces.
pub struct LoopResult<O> {
    /// Latencies recorded during the measured phase only.
    pub histogram: Histogram<u64>,
    /// Interval percentiles over the measured phase.
    pub series: TimeSeries,
    /// When this thread's first measured sample landed. `None` if the
    /// thread recorded nothing.
    pub measured_start: Option<Instant>,
    /// Tallies merged across this thread's connections, covering all
    /// phases.
    pub outcomes: O,
}

/// Run the event loop until the cooldown deadline passes.
pub fn run_loop<W: Workload>(
    mut connections: Vec<Connection<W>>,
    cfg: LoopConfig,
) -> LoopResult<W::Outcomes> {
    let LoopConfig {
        window,
        bench_start,
        deadlines,
        phases,
        progress,
        target_rate,
        total_clients,
        thread_idx,
        total_threads,
        pace_stats,
    } = cfg;

    let ticks_per_ns = calibrate_tsc();

    // `warmup_end_tsc` lets pace_stats.record_send skip telemetry for
    // sends scheduled during warmup. Without this gate, `scheduled` and
    // `late_sends` cover all phases while `achieved_rate` covers
    // measured-only — dividing one by the other in the JSON would
    // overestimate the effective load by the warmup ratio.
    let warmup_end_tsc = if target_rate > 0 {
        let warmup_ticks = (phases.warmup.as_nanos() as f64 * ticks_per_ns) as u64;
        rdtscp().saturating_add(warmup_ticks)
    } else {
        0
    };

    // Materialise pacers now that we have a calibration factor and a
    // local TSC reading. Each connection gets its own scheduler keyed off
    // the same `start_tsc`; the global conn index (which spans threads)
    // staggers the first send across the whole run, not just within one
    // thread.
    if target_rate > 0 {
        let start_tsc = rdtscp();
        let clients = total_clients.max(1) as u64;
        for (local_idx, conn) in connections.iter_mut().enumerate() {
            // Round-robin distribution: this thread's local conn `k` is
            // global conn `thread_idx + k * total_threads`.
            let global_idx = (thread_idx + local_idx * total_threads) as u64;
            conn.pacer = Some(PaceClock::new(
                target_rate,
                clients,
                ticks_per_ns,
                start_tsc,
                global_idx,
            ));
        }
    }

    // 4096 entries: supports up to 1024 connections per thread (RECV +
    // SEND per connection, plus headroom for partial-send resubmissions).
    let mut ring = IoUring::new(4096).expect("create io_uring for bench");
    let mut histogram = new_histogram();
    // Timestamp of the first measured (post-warmup) latency recording.
    // Used to compute throughput over the measured phase only.
    let mut measured_start: Option<Instant> = None;

    let mut interval_hist = new_histogram();
    let mut interval_count: usize = 0;
    // Pre-allocate generously: at 10M req/s × SAMPLE_INTERVAL=1000 across
    // typical bench durations (≤ 10 min) we push ≤ 600k entries; sizing
    // for that up-front avoids the doubling-reallocate spikes that show
    // up as ~100µs outliers in the deep tail at the 32k/64k/128k/256k
    // capacity boundaries.
    let mut series: TimeSeries = Vec::with_capacity(600_000);

    // Pre-allocated CQE collection buffer. Must collect CQEs before
    // processing because the CQ borrow must end before mutating connections.
    // Avoids per-iteration heap allocation from `.collect()`.
    let mut cqes: Vec<(u64, i32)> = Vec::with_capacity(1024);

    // Submit initial RECVs for all connections.
    for (i, conn) in connections.iter_mut().enumerate() {
        let sqe = opcode::Recv::new(
            types::Fd(conn.read_fd),
            conn.recv_buf.as_mut_ptr(),
            RECV_BUF_SIZE as u32,
        )
        .build()
        .user_data(i as u64);
        unsafe {
            ring.submission().push(&sqe).expect("SQ full");
        }
        conn.recv_pending = true;
    }

    // Fill initial send windows.
    fill_windows(
        &mut ring,
        &mut connections,
        window,
        &deadlines,
        &pace_stats,
        ticks_per_ns,
        warmup_end_tsc,
    );

    loop {
        // Wall-clock-driven termination. The histogram is sealed at
        // `measured_end`, so any inflight responses left after we break
        // would only land in cooldown and be discarded anyway.
        if Instant::now() >= deadlines.cooldown_end {
            break;
        }
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => panic!("io_uring submit_and_wait: {e}"),
        }

        // Sample the wall clock *after* the blocking wait and reuse it
        // for the phase classifier on every CQE in this batch. Saves a
        // vDSO call per response — at multi-M ops/s the per-CQE
        // `Instant::now()` (~15-25 ns) was visible in profiles. Outer
        // iters batch many CQEs and phase boundaries are coarse (5 s
        // warmup, 60 s measured), so reusing one timestamp across a
        // batch misclassifies at most a handful of samples around the
        // warmup/measured boundary — far below run-to-run noise.
        let now = Instant::now();

        cqes.clear();
        cqes.extend(ring.completion().map(|cqe| (cqe.user_data(), cqe.result())));

        for &(token, result) in cqes.iter() {
            if token & SEND_FLAG != 0 {
                // ── SEND completion ──
                let idx = (token & !SEND_FLAG) as usize;
                let conn = &mut connections[idx];
                conn.send_pending = false;

                assert!(result >= 0, "send error: {result}");
                let sent = result as usize;
                if sent >= conn.send_buf.len() {
                    conn.send_buf.clear();
                } else {
                    // Partial send — drain and resubmit.
                    conn.send_buf.drain(..sent);
                    let sqe = opcode::Send::new(
                        types::Fd(conn.write_fd),
                        conn.send_buf.as_ptr(),
                        conn.send_buf.len() as u32,
                    )
                    .build()
                    .user_data(idx as u64 | SEND_FLAG);
                    unsafe {
                        ring.submission().push(&sqe).expect("SQ full");
                    }
                    conn.send_pending = true;
                }
            } else {
                // ── RECV completion ──
                let idx = token as usize;
                assert!(result > 0, "recv error or disconnect: {result}");

                let n_bytes = result as usize;
                let conn = &mut connections[idx];
                conn.recv_pending = false;
                conn.parse_buf.extend_from_slice(&conn.recv_buf[..n_bytes]);

                // Parse complete frames.
                let mut cursor = 0;
                while cursor + 4 <= conn.parse_buf.len() {
                    let len_bytes: [u8; 4] = conn.parse_buf[cursor..cursor + 4]
                        .try_into()
                        .expect("4 bytes");
                    let frame_len = u32::from_le_bytes(len_bytes) as usize;
                    if cursor + 4 + frame_len > conn.parse_buf.len() {
                        break;
                    }

                    let frame = &conn.parse_buf[cursor + 4..cursor + 4 + frame_len];
                    let response = conn.workload.decode(frame);
                    cursor += 4 + frame_len;

                    if conn.workload.completes_request(&response) {
                        // `rdtscp()` is captured FIRST — before any
                        // per-frame bookkeeping (outcome tally, parse
                        // buffer compaction) — so the histogram reflects
                        // only the wire roundtrip, not the bench's own
                        // post-processing cost.
                        let sent_tsc = conn.inflight_ts.pop_front().expect(
                            "inflight timestamp desync: got a completion without matching send",
                        );
                        let latency_ns = tsc_to_ns(rdtscp() - sent_tsc, ticks_per_ns);
                        // Phase classification by *receive* time, using
                        // the outer-iter `now`. Once `measured_end`
                        // passes the histogram is sealed; any further
                        // completions fall through silently.
                        if now >= deadlines.warmup_end && now < deadlines.measured_end {
                            if measured_start.is_none() {
                                measured_start = Some(now);
                            }
                            histogram.record(latency_ns).expect("record");
                            interval_hist.record(latency_ns).expect("record interval");
                            interval_count += 1;
                            maybe_sample(
                                &mut interval_hist,
                                &mut interval_count,
                                &mut series,
                                bench_start,
                            );
                            progress.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // Tally outcomes across every phase (warmup,
                    // measured, cooldown). Runs *after* the latency
                    // capture above so the histogram measures the wire
                    // roundtrip only — adding this counter increment
                    // before `rdtscp()` would inflate every sample by
                    // the cost of this call.
                    conn.workload.record(&response);
                }
                if cursor > 0 {
                    // Shift remaining bytes to front without allocating.
                    // `copy_within` + `truncate` avoids the O(n) memmove
                    // overhead of `Vec::drain` which must drop + shift.
                    let remaining = conn.parse_buf.len() - cursor;
                    conn.parse_buf.copy_within(cursor.., 0);
                    conn.parse_buf.truncate(remaining);
                }

                // Re-arm RECV. The outer loop's wall-clock check is the
                // only exit; pending CQEs after cooldown are drained
                // implicitly when the io_uring drops at function exit.
                let sqe = opcode::Recv::new(
                    types::Fd(conn.read_fd),
                    conn.recv_buf.as_mut_ptr(),
                    RECV_BUF_SIZE as u32,
                )
                .build()
                .user_data(idx as u64);
                unsafe {
                    ring.submission().push(&sqe).expect("SQ full");
                }
                conn.recv_pending = true;
            }
        }

        // Refill send windows for connections with capacity.
        fill_windows(
            &mut ring,
            &mut connections,
            window,
            &deadlines,
            &pace_stats,
            ticks_per_ns,
            warmup_end_tsc,
        );
    }

    let mut outcomes = W::Outcomes::default();
    for conn in &connections {
        outcomes.merge(conn.workload.outcomes());
    }

    LoopResult {
        histogram,
        series,
        measured_start,
        outcomes,
    }
}

/// Fill send windows for all connections that have capacity and no pending send.
/// Builds a length-prefixed send buffer and submits SEND SQEs. Stops issuing
/// new frames once the cooldown deadline has passed — the loop above will
/// then terminate as soon as `submit_and_wait` returns (or immediately if
/// the queue is empty).
#[allow(clippy::too_many_arguments)]
fn fill_windows<W: Workload>(
    ring: &mut IoUring,
    connections: &mut [Connection<W>],
    window: usize,
    deadlines: &PhaseDeadlines,
    pace_stats: &PaceStats,
    ticks_per_ns: f64,
    warmup_end_tsc: u64,
) {
    // Past cooldown: do nothing. We want the loop to wind down, not to
    // queue more sends that will arrive after the run is reported.
    if Instant::now() >= deadlines.cooldown_end {
        return;
    }

    for (i, conn) in connections.iter_mut().enumerate() {
        if conn.send_pending {
            continue;
        }

        // Fill the send buffer with as many frames as the window allows.
        // Each frame is encoded directly into `send_buf` as `[u32 LE len][payload]`.
        // When pacing is active, `pop_due` gates each push by the
        // schedule; the recorded timestamp is the *scheduled* TSC, which
        // is what closes the coordinated-omission loophole.
        while conn.inflight_ts.len() < window {
            let send_tsc = if let Some(pacer) = conn.pacer.as_mut() {
                let now_tsc = rdtscp();
                match pacer.pop_due(now_tsc) {
                    Some(scheduled) => {
                        // Gate telemetry on warmup-end so `scheduled` /
                        // `late_sends` reflect the same phase as the
                        // throughput divisor (`achieved_rate`).
                        if now_tsc >= warmup_end_tsc {
                            pace_stats.record_send(now_tsc, scheduled, ticks_per_ns);
                        }
                        scheduled
                    }
                    None => break,
                }
            } else {
                rdtscp()
            };
            conn.workload.next_frame(&mut conn.send_buf);
            conn.inflight_ts.push_back(send_tsc);
        }

        if !conn.send_buf.is_empty() {
            let sqe = opcode::Send::new(
                types::Fd(conn.write_fd),
                conn.send_buf.as_ptr(),
                conn.send_buf.len() as u32,
            )
            .build()
            .user_data(i as u64 | SEND_FLAG);
            unsafe {
                ring.submission().push(&sqe).expect("SQ full");
            }
            conn.send_pending = true;
        }
    }
}
