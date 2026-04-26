//! Smoke test for the rumcast standalone server. Spawns
//! `run_rumcast` in a thread, then uses `melin-rumcast` primitives
//! directly (mimicking what the bench's rumcast roundtrip path does)
//! to publish a single order and verify the BatchEnd response comes
//! back. End-to-end coverage of the order → engine → response path
//! over the rumcast wire format.
//!
//! Only compiled / run when the `rumcast` feature is enabled. Run
//! with: `cargo test -p melin-server --features rumcast --test
//! rumcast_smoke -- --nocapture`.

#![cfg(feature = "rumcast")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};
use melin_rumcast::pub_log::{PublicationConfig, PublicationLog};
use melin_rumcast::receiver::{ReceiverConfig, ReceiverLoop};
use melin_rumcast::sender::{SenderConfig, SenderLoop};
use melin_rumcast::sub_log::{SubscriptionConfig, SubscriptionLog};
use melin_rumcast::transport::KernelUdp;
use melin_rumcast::wire::{FrameView, data_flags};
use melin_server::rumcast_transport::{RumcastConfig, run_rumcast};
use melin_server::server::ServerConfig;
use melin_trading::types::{
    AccountId, Order, OrderId, OrderType, Price, Quantity, SelfTradeProtection, Side, Symbol,
    TimeInForce,
};

// MUST match the constants in melin-server's rumcast_transport.rs and
// melin-bench's rumcast.rs. Mismatch = silent no-traffic.
const RUMCAST_SESSION_ID: u32 = 0xCAFEBABE;
const RUMCAST_ORDERS_STREAM: u32 = 1;
const RUMCAST_RESP_STREAM: u32 = 2;
const TERM_LENGTH: u32 = 16 * 1024 * 1024;
const MTU: u32 = 1408;
const INITIAL_TERM_ID: u32 = 1;
const BENCH_RECEIVER_ID: u64 = 1;

/// Find an unused UDP port by binding ephemeral and dropping.
fn free_udp_port() -> u16 {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap().port()
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

#[test]
fn rumcast_order_round_trip() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let server_port = free_udp_port();
    let bench_resp_port = free_udp_port();
    let server_addr = loopback(server_port);
    let bench_addr = loopback(bench_resp_port);

    // Temp directory for the journal — destroyed when `_tmp` drops.
    let _tmp = tempfile::tempdir().unwrap();
    let journal_path = _tmp.path().join("test.journal");

    // ---- Server config ----
    // Tiny accounts/instruments so seed_and_drain returns quickly.
    let server_config = ServerConfig {
        bind: server_addr,
        journal: journal_path.clone(),
        accounts: 4,
        instruments: 4,
        rumcast_client_addr: Some(bench_addr),
        // Skip the on-disk authorized_keys — rumcast standalone path
        // doesn't authenticate (Phase 1).
        authorized_keys: PathBuf::from("/tmp/non-existent-rumcast-test-keys"),
        ..ServerConfig::default()
    };

    // ---- Spawn server thread ----
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = Arc::clone(&shutdown);
    let server_handle = thread::Builder::new()
        .name("test-rumcast-server".into())
        .spawn(move || {
            // run_rumcast returns Box<dyn Error>; that's not Send, so
            // it can't be returned from the spawn closure as-is.
            // Stringify on the way out.
            run_rumcast(
                server_config,
                RumcastConfig {
                    bind: server_addr,
                    client_addr: bench_addr,
                },
                server_shutdown,
            )
            .map_err(|e| e.to_string())
        })
        .unwrap();

    // Server takes a moment to seed_and_drain + bind. Sleep generously
    // — seed_and_drain at 4 instruments + 4 accounts is microseconds,
    // but the journal create + first fsync can take tens of ms.
    thread::sleep(Duration::from_millis(500));

    // ---- Bench-side rumcast endpoints ----
    let orders_pub = Arc::new(
        PublicationLog::new(PublicationConfig {
            session_id: RUMCAST_SESSION_ID,
            stream_id: RUMCAST_ORDERS_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
            mtu: MTU,
        })
        .unwrap(),
    );
    orders_pub.set_publisher_limit(u64::MAX);
    let orders_socket = KernelUdp::bind(loopback(0)).unwrap();
    let mut orders_send_config = SenderConfig::defaults(server_addr);
    orders_send_config.setup_interval = Duration::from_millis(50);
    orders_send_config.heartbeat_interval = Duration::from_millis(25);
    let mut orders_sender =
        SenderLoop::new(Arc::clone(&orders_pub), orders_socket, orders_send_config);

    let resp_sub = Arc::new(
        SubscriptionLog::new(SubscriptionConfig {
            session_id: RUMCAST_SESSION_ID,
            stream_id: RUMCAST_RESP_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
        })
        .unwrap(),
    );
    let resp_socket = KernelUdp::bind(bench_addr).unwrap();
    let mut resp_recv_config = ReceiverConfig::defaults(server_addr, BENCH_RECEIVER_ID);
    resp_recv_config.sm_interval = Duration::from_millis(50);
    let mut resp_receiver = ReceiverLoop::new(Arc::clone(&resp_sub), resp_socket, resp_recv_config);

    // Bench-side tick threads.
    let tick_shutdown = Arc::new(AtomicBool::new(false));
    let send_tick = {
        let s = Arc::clone(&tick_shutdown);
        thread::spawn(move || {
            while !s.load(Ordering::Acquire) {
                let _ = orders_sender.tick();
                thread::sleep(Duration::from_micros(50));
            }
        })
    };
    let recv_tick = {
        let s = Arc::clone(&tick_shutdown);
        thread::spawn(move || {
            while !s.load(Ordering::Acquire) {
                let _ = resp_receiver.tick();
                thread::sleep(Duration::from_micros(50));
            }
        })
    };

    // ---- Publish one SubmitOrder ----
    // Use account 1, symbol 0 (within the seeded set).
    let order = Order {
        id: OrderId(1),
        account: AccountId(1),
        side: Side::Buy,
        order_type: OrderType::Limit {
            price: Price(NonZeroU64::new(100).unwrap()),
            post_only: false,
        },
        time_in_force: TimeInForce::GTC,
        quantity: Quantity(NonZeroU64::new(10).unwrap()),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    };
    let request = Request::SubmitOrder {
        symbol: Symbol(0),
        order,
    };
    let mut encode_buf = vec![0u8; 256];
    let written = codec::encode_request(&request, /* seq */ 1, &mut encode_buf).unwrap();
    // Strip the 4-byte length prefix — rumcast frames per-message.
    let payload = &encode_buf[4..written];

    // Spin-claim and publish.
    loop {
        match orders_pub.try_claim(payload.len() as u32) {
            Ok(mut claim) => {
                claim.payload_mut().copy_from_slice(payload);
                claim.publish(data_flags::UNFRAGMENTED);
                break;
            }
            Err(_) => std::hint::spin_loop(),
        }
    }

    // ---- Wait for BatchEnd response ----
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got_batch_end = false;
    while Instant::now() < deadline && !got_batch_end {
        resp_sub.poll(64 * 1024, |view| {
            if let FrameView::Data { header, payload } = view
                && header.common.flags & data_flags::PADDING == 0
                && let Ok(kind) = codec::decode_response(payload)
                && matches!(kind, ResponseKind::BatchEnd)
            {
                got_batch_end = true;
            }
        });
        thread::sleep(Duration::from_millis(5));
    }

    // ---- Cleanup ----
    tick_shutdown.store(true, Ordering::Release);
    let _ = send_tick.join();
    let _ = recv_tick.join();
    shutdown.store(true, Ordering::Release);
    // Give the server thread a moment to wind down before joining; it
    // sleeps 100ms between shutdown checks.
    let server_join_deadline = Instant::now() + Duration::from_secs(2);
    while !server_handle.is_finished() && Instant::now() < server_join_deadline {
        thread::sleep(Duration::from_millis(50));
    }
    if server_handle.is_finished() {
        let _ = server_handle.join();
    }

    assert!(
        got_batch_end,
        "did not receive BatchEnd response within 5s — server didn't roundtrip the order"
    );
}
