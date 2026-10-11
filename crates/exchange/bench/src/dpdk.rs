//! DPDK kernel-bypass benchmark client.
//!
//! Replaces the io_uring event loop with a single-threaded poll loop over
//! the sequencer client's DPDK NIC handle (`melin_client::NicHandle`). All
//! connections share the handle's one TCP/IP stack on one DPDK port (or an
//! LACP bond's ports), and the handle dials each of them to the server in
//! turn. The handle owns the NIC: EAL, the mbuf pool, the ports, the
//! device, the interface, the neighbour seeding and the sockets' tuning.
//! The bench keeps its loop: the window, the open-loop pacer, the
//! per-connection keys, the TSC samples and the outcome tally.
//!
//! Each connection is a `melin_client::PolledConnection`, the protocol
//! state with no stream of its own: every call is handed the stream's view
//! (`nic.stream(&id)`, which borrows the handle for that call only) and the
//! bench's own time, so the handshake runs inside `poll` and a request is
//! framed with `send_with` straight into the socket's transmit buffer.
//!
//! This module is only compiled with `--features dpdk`.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use melin_client::{Frame, NicConfig, NicHandle, PeerMacSource, PolledConnection, Reply, StreamId};
use melin_ec_protocol::codec;

use crate::generator;
use crate::{
    BenchPhases, OutcomeReport, TimeSeries, enforce_rejection_threshold, maybe_sample,
    print_results, spawn_progress_reporter,
};

/// TCP socket buffer size, receive and transmit alike. 64KB gives plenty
/// of headroom for pipelined frames in flight.
const SOCKET_BUF_SIZE: usize = 65536;

/// How long one connection's dial may take, SYN to established. The NIC
/// handle needs a deadline; a wrong peer MAC is silent and shows only as a
/// dial that never completes, so the bound turns a hang into an error that
/// names the MAC. Generous next to a LAN round trip, which is microseconds.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a connection's handshake may wait on the node: the challenge,
/// then the verdict. Only the handshake: once a connection is ready, its
/// read timeout is lifted (see [`authenticate`]).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-connection state for the DPDK benchmark.
struct DpdkBenchConn {
    /// The stream this connection runs on, which the NIC handle lends per
    /// call (`nic.stream(&id)`).
    id: StreamId,
    /// The protocol state: the handshake, the receive buffer replies are
    /// split in, and the scratch buffer holding what a send could not hand
    /// the stream at once. Its unsent bytes are the partial-send remainder
    /// the loop drains before framing another request.
    connection: PolledConnection,
    /// Order generator — produces request bodies on-the-fly. No
    /// pre-allocated cap: the bench runs until the wall-clock cooldown
    /// deadline expires. Advanced only when a request is framed, so a
    /// send that frames nothing leaves it where it was, and the next
    /// iteration sends the request that would have gone.
    flow: generator::OrderFlowGenerator,
    /// FIFO of send timestamps (TSC ticks) for in-flight orders.
    /// `u64` instead of `Instant` to avoid ~15-25ns vDSO overhead per
    /// timestamp on the hot path. With pacing enabled this stores the
    /// *scheduled* TSC, not the actual send TSC (coordinated-omission fix).
    inflight_ts: VecDeque<u64>,
    /// Open-loop scheduler when `--target-rate > 0`. Built after auth
    /// handshake so its TSC anchor is close to the measurement start.
    pacer: Option<crate::PaceClock>,
    /// Counts every execution-report variant received on this
    /// connection. Folded into the run-wide [`OutcomeReport`] after the
    /// poll loop exits.
    outcomes: OutcomeReport,
}

/// DPDK benchmark configuration: the NIC, one field per `--dpdk-*` flag,
/// and the server to dial on it.
pub struct DpdkBenchConfig {
    /// The NIC the bench's connections share: the sequencer's own NIC
    /// configuration, the one a node's DPDK transport takes, so the bench
    /// runs in every mode the node does (SR-IOV with a VLAN, an mlx5 port
    /// shared with the kernel, L3 through a gateway). Its fields' docs say
    /// what each flag means for the NIC.
    pub nic: NicConfig,
    pub server_addr: SocketAddr,
}

/// The bench's TSC as the NIC handle's clock: a monotonic nanosecond count
/// from a TSC reading taken just before the handle opens, which the handle
/// requires to be fixed for its life. One calibration converts ticks to
/// nanoseconds for the stack's clock and for the latency samples alike, so
/// the handle needs no clock of its own and the loop reads one counter.
#[derive(Clone, Copy)]
struct BenchClock {
    /// The TSC reading the count starts from.
    origin_tsc: u64,
    /// From the bench's calibration (`crate::calibrate_tsc`). `f64`, as
    /// the calibration and `tsc_to_ns` have it.
    ticks_per_ns: f64,
}

impl BenchClock {
    /// Nanoseconds from the origin to a TSC reading the caller already
    /// took. A reading before the origin (an unsynchronised counter on
    /// another core) counts as the origin; the stack clamps a reading
    /// that goes backwards anyway.
    #[inline(always)]
    fn ns_at(&self, tsc: u64) -> u64 {
        crate::tsc_to_ns(tsc.saturating_sub(self.origin_tsc), self.ticks_per_ns)
    }

    /// Nanoseconds from the origin to now.
    #[inline(always)]
    fn now_ns(&self) -> u64 {
        self.ns_at(crate::rdtscp())
    }
}

/// A frame from a polled connection as the outcome tally takes it. A
/// heartbeat never surfaces from `poll`, so `Reply::Heartbeat`, which the
/// tally does not count, never comes from here.
fn reply_of(frame: Frame<'_>) -> Reply<'_> {
    match frame {
        Frame::Response(body) => Reply::Response(body),
        Frame::BatchEnd => Reply::BatchEnd,
        Frame::ServerBusy => Reply::ServerBusy,
        Frame::EngineError => Reply::EngineError,
    }
}

/// Whether the stream `id` names would queue anything now. An error is
/// only an id another handle dialed, which this module never makes; it is
/// logged and taken as no room, which stops sending on that connection.
#[inline]
fn can_send(nic: &mut NicHandle, id: &StreamId) -> bool {
    match nic.stream(id).can_send() {
        Ok(room) => room,
        Err(e) => {
            tracing::debug!("dpdk can_send error: {e:?}");
            false
        }
    }
}

/// Run the handshake of a freshly dialed connection to its end, servicing
/// the NIC on the bench's clock meanwhile. The handshake runs inside
/// `poll`, which answers the node's challenge as it arrives and makes the
/// connection ready on the node's verdict; nothing else is sent until then.
///
/// Once ready, the connection's read timeout is lifted: the bench's loop
/// has always waited on the node for as long as the run lasts, and a slow
/// reply is a sample, not an error.
fn authenticate(
    nic: &mut NicHandle,
    id: &StreamId,
    connection: &mut PolledConnection,
    clock: &BenchClock,
) -> Result<(), melin_client::Error> {
    while !connection.is_ready() {
        let now_ns = clock.now_ns();
        nic.service(now_ns);
        if let Some(frame) = connection.poll(&mut nic.stream(id), now_ns)? {
            // Nothing was sent but the challenge response, so nothing but
            // the handshake is owed.
            return Err(melin_client::Error::Protocol(format!(
                "a reply ({frame:?}) before any request"
            )));
        }
    }
    connection.set_read_timeout(Duration::MAX);
    Ok(())
}

/// Run the DPDK roundtrip benchmark.
///
/// # Errors
///
/// A NIC that cannot be brought up, a server address that is not IPv4, a
/// dial that fails (a timeout, whose message names the MAC the SYN was
/// addressed to, or a refusal), or a handshake the node refuses or does not
/// answer. The message is the one to print; the NIC, and with it every
/// connection already dialed, is closed by then.
#[allow(clippy::too_many_arguments)]
pub fn run_dpdk_roundtrip(
    max_reject_pct: f64,
    config: DpdkBenchConfig,
    phases: BenchPhases,
    window: usize,
    num_clients: usize,
    json_path: Option<&std::path::Path>,
    key: &ed25519_dalek::SigningKey,
    num_accounts: u32,
    num_instruments: u32,
    core_id: usize,
    health_addr: Option<std::net::SocketAddr>,
    target_rate: u64,
) -> Result<(), String> {
    // Pin to dedicated core.
    if let Err(e) = melin_app::affinity::pin_to_core(core_id) {
        eprintln!("warning: could not pin bench to core {core_id}: {e}");
    }

    let server_addr = match config.server_addr {
        SocketAddr::V4(v4) => v4,
        SocketAddr::V6(_) => return Err("IPv6 not supported".to_owned()),
    };

    // Calibrated before the NIC opens rather than just before the loop:
    // the handle's stack runs on the bench's TSC from its first dial, so
    // the dials and the loop share one clock and one conversion.
    let ticks_per_ns = crate::calibrate_tsc();
    let clock = BenchClock {
        origin_tsc: crate::rdtscp(),
        ticks_per_ns,
    };

    // EAL, the mbuf pool, the ports (each with its steering rule in
    // bifurcated mode), the device and the interface, as a node brings
    // them up. Outside L3 mode the handle announces itself with a
    // gratuitous ARP as it opens.
    let mut nic =
        NicHandle::open(&config.nic).map_err(|e| format!("DPDK NIC setup failed: {e}"))?;
    if config.nic.mtu != 1500 {
        eprintln!("  DPDK jumbo frames: MTU {}", config.nic.mtu);
    }

    // Two seeding strategies, picked by mode, both the handle's:
    //   - L3 bifurcated: the stack routes the server IP via the gateway,
    //     and the gateway's ARP reply has a different source IP than
    //     `peer_ip`, so it never reaches DPDK. The handle seeds (gateway,
    //     gateway_mac) statically — the operator pulled it from `ip neigh`.
    //   - Legacy SR-IOV: VFs can't receive broadcast ARP, so the stack
    //     can't resolve the server either. The handle sends a gratuitous
    //     ARP for the switch's benefit and seeds the server's MAC on each
    //     dial: the supplied one, or the deterministic VF MAC
    //     (02:00:IP[0..4]).
    if let (Some(gw), Some(gw_mac)) = (config.nic.gateway, config.nic.gateway_mac) {
        eprintln!(
            "  ARP: seeded gateway {gw} = {:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x} (L3 mode)",
            gw_mac[0], gw_mac[1], gw_mac[2], gw_mac[3], gw_mac[4], gw_mac[5],
        );
    } else {
        // Name the source: a derived MAC on a port that keeps its real
        // hardware address is the failure mode this path is most likely
        // to hit, and it is otherwise indistinguishable from success —
        // the connect just times out, with this MAC in its error.
        let (server_mac, source) = nic.peer_mac(*server_addr.ip());
        let mac_source = match source {
            PeerMacSource::Supplied => "supplied",
            PeerMacSource::DerivedSrIov => "derived 02:00:<ip>",
        };
        eprintln!(
            "  ARP: sent gratuitous, seeded server MAC \
             {:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x} ({mac_source})",
            server_mac[0],
            server_mac[1],
            server_mac[2],
            server_mac[3],
            server_mac[4],
            server_mac[5]
        );
    }

    // --- Connect, auth, and set up each client sequentially ---
    //
    // Each connection is dialed and authenticated before the next is
    // dialed. A DPDK node keeps one listening socket per port and answers
    // a SYN that arrives during another connection's handshake with an
    // RST, so dials in flight together would be refused, not queued; the
    // NIC handle's dial returns only once its connection is established.
    // Build a generator per client. Phases are driven by wall-clock
    // deadlines on the shared `start` instant, so there is no
    // per-client total to compute — generators run indefinitely.
    //
    // ORDER_ID_STRIDE matches the io_uring path: 2^48 ids per client,
    // more than three orders of magnitude beyond any realistic run at
    // 10 M/s.
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

    // Per-client signing keys — see the matching comment in main.rs.
    // The engine dedups by `(key_hash, request_seq)` with a single
    // per-key HWM; sharing one key collapses every connection into one
    // namespace and the leader's HWM rejects everyone else's requests.
    let client_keys: Vec<ed25519_dalek::SigningKey> = (0..num_clients)
        .map(|i| crate::keys::derive_client_key(key, i as u32))
        .collect();

    let dial_timeout_ns = DIAL_TIMEOUT.as_nanos() as u64;
    let mut connections: Vec<DpdkBenchConn> = Vec::with_capacity(num_clients);
    let setup_start = Instant::now();
    eprintln!("  connecting {num_clients} clients via DPDK...");
    for (client_id, flow) in per_client.into_iter().enumerate() {
        // Each dial takes a fresh local port from the handle, so a run
        // never reuses the four-tuple of a connection the server may
        // still hold from a previous run.
        let deadline_ns = clock.now_ns().saturating_add(dial_timeout_ns);
        let id = nic
            .dial(server_addr, SOCKET_BUF_SIZE, deadline_ns, || clock.now_ns())
            .map_err(|e| format!("connect failed for client {client_id}: {e}"))?;

        let mut connection =
            PolledConnection::with_timeout(&client_keys[client_id], HANDSHAKE_TIMEOUT);
        authenticate(&mut nic, &id, &mut connection, &clock)
            .map_err(|e| format!("client {client_id}: auth handshake failed: {e}"))?;

        connections.push(DpdkBenchConn {
            id,
            connection,
            flow,
            inflight_ts: VecDeque::with_capacity(window),
            pacer: None,
            outcomes: OutcomeReport::default(),
        });

        eprintln!("  client {}/{num_clients}: connected", client_id + 1);
    }
    eprintln!(
        "  all {num_clients} clients ready ({:.1}s setup)",
        setup_start.elapsed().as_secs_f64(),
    );

    // --- Main benchmark loop ---
    let progress = Arc::new(AtomicU64::new(0));
    let progress_shutdown = Arc::new(AtomicBool::new(false));
    let pace_stats = Arc::new(crate::PaceStats::default());
    let progress_handle = spawn_progress_reporter(
        Arc::clone(&progress),
        phases,
        Arc::clone(&progress_shutdown),
        target_rate,
        Arc::clone(&pace_stats),
    );

    let health_poller = health_addr.map(crate::health_poller::HealthPoller::start);

    let start = Instant::now();
    let deadlines = phases.deadlines(start);
    let mut measured_start: Option<Instant> = None;

    // Build per-connection pacers anchored at the bench-start TSC. With
    // the DPDK loop running single-threaded over all connections, a
    // shared anchor is sufficient; staggered first sends via
    // `conn_index` keep the connections out of phase within one period.
    // `warmup_end_tsc` gates pace_stats telemetry so `scheduled` /
    // `late_sends` cover the same phase as `achieved_rate`.
    let warmup_end_tsc: u64 = if target_rate > 0 {
        let start_tsc = crate::rdtscp();
        let clients = num_clients.max(1) as u64;
        for (idx, conn) in connections.iter_mut().enumerate() {
            conn.pacer = Some(crate::PaceClock::new(
                target_rate,
                clients,
                ticks_per_ns,
                start_tsc,
                idx as u64,
            ));
        }
        let warmup_ticks = (phases.warmup.as_nanos() as f64 * ticks_per_ns) as u64;
        start_tsc.saturating_add(warmup_ticks)
    } else {
        0
    };

    let mut histogram =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
    let mut interval_hist =
        Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("interval histogram");
    let mut interval_count: usize = 0;
    let mut series: TimeSeries = Vec::new();

    // Outer-loop wall-time histogram, gated on at least one BatchEnd
    // received this iteration AND on being past warmup so we measure
    // steady-state.
    #[cfg(feature = "latency-trace")]
    let mut poll_iter_hist =
        Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).expect("poll iter histogram bounds");

    // The NIC's service step (device RX poll, the stack's poll over every
    // socket, TX flush) as one call, timed whole: the handle does the
    // three in one step. Recorded only on the call at the top of each
    // main-loop iteration, never on connect-phase calls.
    #[cfg(feature = "latency-trace")]
    let mut service_hist =
        Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).expect("service hist bounds");

    // Poll every N connections to keep the NIC busy during the
    // connection iteration. Without this, the stack's TX buffer fills
    // up with 16 connections at window 256, and the NIC sits idle
    // until the next service step at the top of the loop.
    const POLL_EVERY_N_CONNS: usize = 4;

    let mut diag_sends: u64 = 0;
    let mut diag_batch_ends: u64 = 0;
    let mut diag_cant_send: u64 = 0;
    let mut diag_pacer_none: u64 = 0;
    let mut diag_window_full: u64 = 0;

    loop {
        // Stamp the start of each outer iter for the poll_iter histogram.
        // Cheap: rdtsc is a few cycles. The compiler elides this when the
        // feature is off (MonoTraceInstant = ()).
        #[cfg(feature = "latency-trace")]
        let iter_start_tsc = crate::rdtscp();
        #[cfg(feature = "latency-trace")]
        let mut work_done_this_iter = false;

        // The NIC's service step, once per iteration, at the bench's TSC
        // time. The same reading is the time the connections are handed
        // below; it only ever starts a read deadline, and none runs once
        // the connections are ready.
        let service_tsc = crate::rdtscp();
        let now_ns = clock.ns_at(service_tsc);
        nic.service(now_ns);
        #[cfg(feature = "latency-trace")]
        service_hist
            .record(crate::tsc_to_ns(
                crate::rdtscp() - service_tsc,
                ticks_per_ns,
            ))
            .ok();

        // Sample the wall clock once per outer iter and reuse it for
        // both the cooldown break and the per-completion phase
        // classifier below. Saves one vDSO call (~15-25 ns) per
        // BatchEnd; at multi-M ops/s the per-completion `Instant::now()`
        // was visible in profiles. Phase boundaries are coarse (5 s
        // warmup, 60 s measured) so the reused timestamp misclassifies
        // at most a handful of samples on a boundary — orders of
        // magnitude below run-to-run noise.
        //
        // Wall-clock-driven termination: stop once cooldown ends. The
        // histogram is sealed at `measured_end` so any responses that
        // would arrive in the cooldown tail are already discarded by
        // the phase classifier; abandoning them doesn't change the
        // reported latencies.
        let now = Instant::now();
        if now >= deadlines.cooldown_end {
            break;
        }

        for (i, conn) in connections.iter_mut().enumerate() {
            // Mid-iteration service step to flush TX and receive new data.
            if i > 0 && i % POLL_EVERY_N_CONNS == 0 {
                nic.service(clock.now_ns());
            }

            // --- Send: fill the window ---
            // First, drain any pending partial send: what the stream did
            // not take of an earlier request, held by the connection.
            if conn.connection.unsent() > 0
                && let Err(e) = conn.connection.flush(&mut nic.stream(&conn.id))
            {
                // Client-side TCP error — debug! per project log-level
                // convention. The bytes stay queued for the next try.
                tracing::debug!("dpdk flush error: {e:?}");
            }

            // Send new frames while window has room. Each request is
            // generated on-the-fly and framed straight into the socket's
            // transmit buffer (`send_with` over the stream's in-place
            // send). When the socket has no room, nothing is generated
            // and `inflight_ts` is *not* advanced — so the recorded send
            // timestamp reflects the moment bytes actually start hitting
            // the wire, not the moment generation completed. A request
            // the contiguous room cannot hold whole is framed in the
            // connection's scratch buffer and as much of it as fits goes
            // out now; the remainder is the pending partial send above.
            //
            // With pacing, `pop_due` gates each new frame on the schedule
            // and the recorded timestamp is the *scheduled* tick (closes
            // the coordinated-omission loophole).
            if !can_send(&mut nic, &conn.id) {
                diag_cant_send += 1;
            } else if conn.inflight_ts.len() >= window {
                diag_window_full += 1;
            }
            while conn.connection.unsent() == 0
                && conn.inflight_ts.len() < window
                && can_send(&mut nic, &conn.id)
            {
                // Pacing gate first — if the next slot isn't due yet,
                // stop filling for this conn this iteration.
                let paced_ts = if let Some(p) = conn.pacer.as_mut() {
                    let now_tsc = crate::rdtscp();
                    match p.pop_due(now_tsc) {
                        Some(scheduled) => {
                            // Gate telemetry on warmup-end so `scheduled` /
                            // `late_sends` align with the
                            // measured-phase `achieved_rate` divisor.
                            if now_tsc >= warmup_end_tsc {
                                pace_stats.record_send(now_tsc, scheduled, ticks_per_ns);
                            }
                            Some(scheduled)
                        }
                        None => {
                            diag_pacer_none += 1;
                            break;
                        }
                    }
                } else {
                    None
                };

                // The generator advances only inside the encoder, which
                // runs once if the request is framed and not at all
                // otherwise, so a send that frames nothing loses no
                // request. A generated request always encodes; a failure
                // there is a bug in the generator or the codec.
                let mut framed = false;
                let flow = &mut conn.flow;
                let sent = conn.connection.send_with(
                    &mut nic.stream(&conn.id),
                    now_ns,
                    codec::MAX_REQUEST_BODY,
                    |body: &mut [u8]| {
                        framed = true;
                        Ok::<usize, melin_client::Error>(
                            flow.next_request_body(body).expect("encode"),
                        )
                    },
                );
                match sent {
                    Ok(()) => {
                        // Framed in place, or framed in the scratch buffer
                        // and flushed. `send_with` alone does not promise
                        // the flush took anything (it may take 0 bytes);
                        // what does is the loop's `can_send` gate above:
                        // with room in the send buffer, the stream's send
                        // takes at least one byte. So the request has
                        // started going out, and it counts. Keep that gate
                        // ahead of this send: without it, a request still
                        // parked in the connection would be timestamped
                        // here.
                        conn.inflight_ts
                            .push_back(paced_ts.unwrap_or_else(crate::rdtscp));
                        diag_sends += 1;
                    }
                    Err(e) => {
                        // Client-side TCP error — debug! per project log-level
                        // convention (silent at default RUST_LOG level so a
                        // noisy network doesn't spam stderr).
                        tracing::debug!("dpdk send error: {e:?}");
                        if framed {
                            // The request is queued in the connection and
                            // goes out with the next flush that succeeds:
                            // it counts, so the FIFO stays aligned with
                            // what the server will acknowledge.
                            conn.inflight_ts
                                .push_back(paced_ts.unwrap_or_else(crate::rdtscp));
                            diag_sends += 1;
                        } else if paced_ts.is_some()
                            && let Some(p) = conn.pacer.as_mut()
                        {
                            // Nothing was generated: roll back the pacer
                            // so the same scheduled slot is re-issued for
                            // the next request.
                            p.unpop();
                        }
                        break;
                    }
                }
            }

            // --- Recv: replies, split where they were received ---
            loop {
                // Malformed frames are dropped intentionally, here and
                // in the outcome tally below: they are not the bench's
                // responsibility to diagnose, and panicking would mask
                // genuine throughput regressions during a long run. A
                // frame no reply carries is consumed, so the next poll
                // moves on. Any other error ends this connection's
                // receive for the iteration and comes back on the next:
                // a length over the limit poisons the connection, which
                // only repeats it, and a stream the server closed or
                // that failed has nothing more to give. Neither is
                // logged, as the loop meets it once per iteration; the
                // connection's line in the diagnostics after the run
                // shows its requests left in flight.
                let frame = match conn.connection.poll(&mut nic.stream(&conn.id), now_ns) {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(melin_client::Error::Protocol(_)) => continue,
                    Err(_) => break,
                };
                if matches!(frame, Frame::BatchEnd) {
                    diag_batch_ends += 1;
                    // Capture `rdtscp()` BEFORE any per-frame
                    // bookkeeping (outcome tally below) so the
                    // histogram reflects only the wire roundtrip.
                    // Always pop the inflight entry to keep the FIFO
                    // aligned with sends — without this, cooldown
                    // completions would leak the queue. Phase
                    // classification by *receive* time, reusing the
                    // outer-iter `now`: past `measured_end` we
                    // discard the sample, before `warmup_end` too.
                    if let Some(sent_tsc) = conn.inflight_ts.pop_front()
                        && now >= deadlines.warmup_end
                        && now < deadlines.measured_end
                    {
                        if measured_start.is_none() {
                            measured_start = Some(now);
                        }
                        let latency_ns = crate::tsc_to_ns(crate::rdtscp() - sent_tsc, ticks_per_ns);
                        histogram.record(latency_ns).ok();
                        interval_hist.record(latency_ns).ok();
                        interval_count += 1;
                        maybe_sample(&mut interval_hist, &mut interval_count, &mut series, start);
                        progress.fetch_add(1, Ordering::Relaxed);
                        #[cfg(feature = "latency-trace")]
                        {
                            work_done_this_iter = true;
                        }
                    }
                }
                // Outcome tally runs *after* the latency capture
                // above so this counter increment is not billed to
                // the wire roundtrip. An undecodable response is
                // dropped for the reason given above.
                let _ = conn.outcomes.record(&reply_of(frame));
            }
        }

        // Record this iteration's wall-time if it produced any measured
        // latency samples. Skip warmup/idle iters so the percentiles
        // describe steady-state poll behaviour, not connection setup.
        #[cfg(feature = "latency-trace")]
        if work_done_this_iter {
            let elapsed_ns = crate::tsc_to_ns(crate::rdtscp() - iter_start_tsc, ticks_per_ns);
            poll_iter_hist.record(elapsed_ns).ok();
        }
    }

    // Snapshot end time BEFORE joining the progress thread: that thread
    // sleeps in 5-second increments and only checks shutdown after each
    // sleep, so progress_handle.join() can block up to ~5s and would
    // otherwise inflate `measured_wall` for short benches.
    let end = Instant::now();

    // Stop progress reporter.
    progress_shutdown.store(true, Ordering::Relaxed);
    let _ = progress_handle.join();

    eprintln!(
        "\n=== DPDK Bench Diagnostics ===\n  \
         total sends:      {diag_sends}\n  \
         total batch_ends: {diag_batch_ends}\n  \
         cant_send hits:   {diag_cant_send}\n  \
         pacer_none hits:  {diag_pacer_none}\n  \
         window_full hits: {diag_window_full}"
    );
    for (ci, conn) in connections.iter().enumerate() {
        eprintln!(
            "  conn[{ci}]: inflight={} pending={}",
            conn.inflight_ts.len(),
            conn.connection.unsent(),
        );
    }

    let measured_wall = measured_start
        .map(|s| end.duration_since(s).min(phases.measured))
        .unwrap_or_else(|| start.elapsed());

    series.sort_by(|a, b| a.elapsed_secs.partial_cmp(&b.elapsed_secs).unwrap());

    let mut extra_lines = vec![
        "  Transport: DPDK (smoltcp)".to_owned(),
        format!("  DPDK core: {core_id}"),
        format!("  Window: {window}, Clients: {num_clients}"),
    ];
    let pacing_report = if target_rate > 0 {
        let scheduled = pace_stats.scheduled.load(Ordering::Relaxed);
        let late = pace_stats.late_sends.load(Ordering::Relaxed);
        let max_delay_us = crate::tsc_to_ns(
            pace_stats.max_send_delay_ticks.load(Ordering::Relaxed),
            ticks_per_ns,
        ) as f64
            / 1_000.0;
        extra_lines.push(format!(
            "  Target rate: {target_rate} ops/s (scheduled {scheduled}, late {late}, max send delay {max_delay_us:.1} µs)"
        ));
        Some(crate::PacingReport {
            target_rate,
            scheduled,
            late_sends: late,
            max_send_delay_us: max_delay_us,
        })
    } else {
        None
    };

    #[cfg(feature = "latency-trace")]
    {
        let us = |ns: u64| ns as f64 / 1000.0;
        let report = |name: &str, h: &Histogram<u64>| {
            if h.is_empty() {
                return;
            }
            eprintln!(
                "  {name}\n\
                 \x20   samples: {samples}\n\
                 \x20   min:    {min:>8.2} µs\n\
                 \x20   p50:    {p50:>8.2} µs\n\
                 \x20   p90:    {p90:>8.2} µs\n\
                 \x20   p99:    {p99:>8.2} µs\n\
                 \x20   p99.9:  {p999:>8.2} µs\n\
                 \x20   p99.99: {p9999:>8.2} µs\n\
                 \x20   max:    {max:>8.2} µs",
                samples = h.len(),
                min = us(h.min()),
                p50 = us(h.value_at_quantile(0.50)),
                p90 = us(h.value_at_quantile(0.90)),
                p99 = us(h.value_at_quantile(0.99)),
                p999 = us(h.value_at_quantile(0.999)),
                p9999 = us(h.value_at_quantile(0.9999)),
                max = us(h.max()),
            );
        };
        report(
            "bench poll: outer iteration (work-iters only)",
            &poll_iter_hist,
        );
        report("bench poll: nic.service()", &service_hist);
    }

    // Fetch the server-side per-stage histogram dump before the
    // server shuts down. Best-effort; missing data is rendered as a
    // one-line note in print_results.
    let server_stages = match health_addr {
        Some(addr) => crate::stats_client::fetch(addr),
        None => crate::stats_client::Body::Empty,
    };

    // Fold per-connection outcome counters. Single-threaded DPDK loop so
    // no atomics needed; we just sum across the conn vector.
    let mut outcomes = OutcomeReport::default();
    for conn in &connections {
        outcomes.merge(&conn.outcomes);
    }

    print_results(
        "Roundtrip",
        histogram.len() as usize,
        phases,
        &histogram,
        measured_wall,
        &extra_lines,
        json_path,
        &series,
        &health_poller.map(|p| p.stop()).unwrap_or_default(),
        &server_stages,
        pacing_report.as_ref(),
        Some(&outcomes),
    );

    // Close the NIC before the rejection gate, which may exit the
    // process: every stream is reset, so the server frees the
    // connections' slots at once rather than at its next heartbeat, and
    // EAL is cleaned up.
    drop(connections);
    drop(nic);

    enforce_rejection_threshold(&outcomes, max_reject_pct);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_clock_counts_nanoseconds_from_its_origin() {
        let clock = BenchClock {
            origin_tsc: 1_000,
            ticks_per_ns: 2.0,
        };
        assert_eq!(clock.ns_at(1_000), 0);
        assert_eq!(clock.ns_at(3_000), 1_000);
        // A reading before the origin counts as the origin.
        assert_eq!(clock.ns_at(10), 0);
    }

    #[test]
    fn the_clock_never_goes_backwards() {
        let clock = BenchClock {
            origin_tsc: crate::rdtscp(),
            ticks_per_ns: crate::calibrate_tsc(),
        };
        let mut last = clock.now_ns();
        for _ in 0..10_000 {
            let now = clock.now_ns();
            assert!(now >= last, "{now} after {last}");
            last = now;
        }
    }

    /// Every frame a polled connection surfaces reaches the tally as the
    /// reply it is, so the outcome counts are the ones the decoder-fed
    /// loop kept.
    #[test]
    fn each_frame_is_tallied_as_its_reply() {
        let body = [1u8, 2, 3];
        assert_eq!(reply_of(Frame::Response(&body)), Reply::Response(&body));
        assert_eq!(reply_of(Frame::BatchEnd), Reply::BatchEnd);
        assert_eq!(reply_of(Frame::ServerBusy), Reply::ServerBusy);
        assert_eq!(reply_of(Frame::EngineError), Reply::EngineError);

        let mut outcomes = OutcomeReport::default();
        for frame in [Frame::BatchEnd, Frame::ServerBusy, Frame::EngineError] {
            outcomes.record(&reply_of(frame)).unwrap();
        }
        assert_eq!(outcomes.batch_ends, 1);
        assert_eq!(outcomes.server_busy, 1);
        assert_eq!(outcomes.engine_errors, 1);
    }
}
