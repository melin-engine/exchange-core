//! End-to-end pipelined benchmark for the trading engine.
//!
//! Boots the server in-process, connects via TCP (default) or Unix domain
//! socket (`--uds`), and blasts order pairs (buy then sell at the same
//! price from the same account — self-trade, net zero balance change,
//! unlimited cycles).
//!
//! Uses closed-loop windowed pipelining: maintains a fixed number of
//! in-flight orders to keep the pipeline saturated without unbounded
//! queue buildup. Measures per-order round-trip latency under load.
//!
//! Usage:
//!     cargo run --release -p trading-bench [-- [--uds] <order_pairs>]
//!
//! Default: TCP transport, 1,000,000 order pairs (2,000,000 total orders).

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use hdrhistogram::Histogram;
use tokio::sync::mpsc as tokio_mpsc;

use trading_engine::types::*;
use trading_protocol::codec;
use trading_protocol::message::{Request, ResponseKind};
use trading_protocol::transport::{TransportRead, TransportStream, TransportWrite};
use trading_server::server::ServerConfig;

/// Number of order pairs (buy + sell) per benchmark run.
const DEFAULT_PAIRS: usize = 1_000_000;

/// Warmup orders (not measured) to prime the pipeline and caches.
const WARMUP_ORDERS: usize = 1_000;

/// Number of orders in flight simultaneously. Controls the level of
/// pipelining — enough to keep the server pipeline saturated (journal +
/// matching stages overlap), small enough that per-order latency reflects
/// actual processing time rather than unbounded queueing.
const WINDOW: usize = 64;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let use_uds = args.iter().any(|a| a == "--uds");
    let pairs: usize = args
        .iter()
        .filter(|a| *a != "--uds")
        .find_map(|s| s.parse().ok())
        .unwrap_or(DEFAULT_PAIRS);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(run_benchmark(pairs, use_uds));
}

async fn run_benchmark(pairs: usize, use_uds: bool) {
    let tmp_dir = tempdir();
    let journal_path = tmp_dir.join("bench.journal");

    let config = ServerConfig {
        journal_path,
        snapshot_path: None,
        ..ServerConfig::default()
    };

    // Shared shutdown flag — set after benchmark completes so pipeline
    // threads can clean up and print latency-trace reports.
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_for_server = Arc::clone(&shutdown);

    // Set up transport and run the benchmark. Each branch calls
    // run_bench_loop with its concrete transport types (monomorphized).
    if use_uds {
        let (reader, writer, name) = setup_uds(&tmp_dir, config, shutdown_for_server).await;
        run_bench_loop(reader, writer, name, pairs, shutdown).await;
    } else {
        let (reader, writer, name) = setup_tcp(config, shutdown_for_server).await;
        run_bench_loop(reader, writer, name, pairs, shutdown).await;
    };

    // Cleanup temp directory.
    let _ = std::fs::remove_dir_all(&tmp_dir);
}

/// Set up TCP transport: bind listener, spawn server, connect client.
async fn setup_tcp(
    config: ServerConfig,
    shutdown: Arc<AtomicBool>,
) -> (impl TransportRead, impl TransportWrite, &'static str) {
    use trading_protocol::tcp::{TcpTransportListener, TcpTransportStream};

    let bind_addr: SocketAddr = "127.0.0.1:0".parse().expect("valid addr");
    let listener = TcpTransportListener::bind(bind_addr)
        .await
        .expect("failed to bind TCP");
    let actual_addr = listener.local_addr().expect("listener has local addr");

    let _server_handle = tokio::spawn(async move {
        if let Err(e) = trading_server::server::run_with_shutdown(listener, config, shutdown).await
        {
            eprintln!("server error: {e}");
        }
    });

    // Connect with retry.
    let stream = {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match tokio::net::TcpStream::connect(actual_addr).await {
                Ok(s) => break s,
                Err(_) if attempts < 50 => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(e) => panic!("failed to connect after 50 attempts: {e}"),
            }
        }
    };
    stream.set_nodelay(true).expect("set TCP_NODELAY");
    let transport = TcpTransportStream::new(stream);
    let (reader, writer) = transport.into_split();

    (reader, writer, "TCP loopback")
}

/// Set up UDS transport: bind listener, spawn server, connect client.
async fn setup_uds(
    tmp_dir: &std::path::Path,
    config: ServerConfig,
    shutdown: Arc<AtomicBool>,
) -> (impl TransportRead, impl TransportWrite, &'static str) {
    use trading_protocol::uds::{UdsTransportListener, UdsTransportStream};

    let sock_path = tmp_dir.join("bench.sock");
    let listener = UdsTransportListener::bind(&sock_path).expect("failed to bind UDS");

    let _server_handle = tokio::spawn(async move {
        if let Err(e) = trading_server::server::run_with_shutdown(listener, config, shutdown).await
        {
            eprintln!("server error: {e}");
        }
    });

    // Connect with retry.
    let stream = {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match tokio::net::UnixStream::connect(&sock_path).await {
                Ok(s) => break s,
                Err(_) if attempts < 50 => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(e) => panic!("failed to connect after 50 attempts: {e}"),
            }
        }
    };
    let transport = UdsTransportStream::new(stream);
    let (reader, writer) = transport.into_split();

    (reader, writer, "Unix domain socket")
}

/// Run the core benchmark loop: encode, send, receive, report.
async fn run_bench_loop(
    mut reader: impl TransportRead,
    mut writer: impl TransportWrite,
    transport_name: &str,
    pairs: usize,
    shutdown: Arc<AtomicBool>,
) {
    let total_orders = WARMUP_ORDERS + (pairs * 2);
    let nz = |v: u64| NonZeroU64::new(v).expect("non-zero");

    // Pre-encode all request frames.
    // Alternating buy/sell at the same price from Account 1 creates
    // self-trades with net zero balance change — unlimited cycles.
    let mut encoded_frames: Vec<Vec<u8>> = Vec::with_capacity(total_orders);
    let mut encode_buf = [0u8; 128];

    for i in 0..total_orders {
        let order_id = OrderId((i as u64) + 1);
        let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };

        let request = Request::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: order_id,
                account: AccountId(1),
                side,
                order_type: OrderType::Limit {
                    price: Price(nz(100)),
                },
                time_in_force: TimeInForce::GTC,
                quantity: Quantity(nz(1)),
            },
        };

        let written = codec::encode_request(&request, &mut encode_buf).expect("encode");
        encoded_frames.push(encode_buf[4..written].to_vec());
    }

    // --- Closed-loop windowed pipelining ---
    //
    // Bounded timestamp channel acts as flow control: the sender blocks
    // when WINDOW orders are in-flight. The receiver pops a timestamp on
    // each BatchEnd, unblocking the sender to send the next order.
    // This keeps the pipeline saturated without unbounded queue buildup.
    let (ts_tx, mut ts_rx) = tokio_mpsc::channel::<Instant>(WINDOW);

    // Spawn receiver: reads responses, records per-order latency on each BatchEnd.
    let recv_handle = tokio::spawn(async move {
        // Histogram range: 1 ns to 10 s, 3 significant digits.
        let mut histogram =
            Histogram::<u64>::new_with_bounds(1, 10_000_000_000, 3).expect("histogram bounds");
        let mut batch_count: usize = 0;

        loop {
            let frame = reader
                .read_frame()
                .await
                .expect("read_frame")
                .expect("server disconnected unexpectedly");

            let response = codec::decode_response(&frame).expect("decode response");
            if matches!(response, ResponseKind::BatchEnd) {
                let sent_at = ts_rx.recv().await.expect("timestamp channel closed");
                let latency_ns = sent_at.elapsed().as_nanos() as u64;

                // Skip warmup orders.
                if batch_count >= WARMUP_ORDERS {
                    histogram.record(latency_ns).expect("record");
                }

                batch_count += 1;
                if batch_count >= total_orders {
                    break;
                }
            }
        }

        histogram
    });

    // Sender: pushes timestamp then frame. Blocks when WINDOW orders are
    // in-flight (bounded channel backpressure). Flushes periodically and
    // always before the channel might block (when approaching window capacity).
    let blast_start = Instant::now();
    let mut unflushed: usize = 0;

    for frame in &encoded_frames {
        ts_tx.send(Instant::now()).await.expect("timestamp send");
        writer.write_frame(frame).await.expect("write_frame");
        unflushed += 1;

        // Flush when we've accumulated a batch or when the window is
        // nearly full (to avoid deadlock — the receiver needs to see
        // frames to drain the window and unblock the sender).
        if unflushed >= 16 || ts_tx.capacity() == 0 {
            writer.flush().await.expect("flush");
            unflushed = 0;
        }
    }
    if unflushed > 0 {
        writer.flush().await.expect("flush");
    }

    // Wait for all responses and get the histogram back.
    let histogram = recv_handle.await.expect("receiver task panicked");
    let blast_duration = blast_start.elapsed();

    // --- Report ---
    let measured_orders = pairs * 2;
    // Throughput uses total_orders (including warmup) since blast_duration
    // covers the entire run. The pipeline is warm for all but the first
    // few hundred orders, so this is representative of steady state.
    let throughput = (total_orders as f64) / blast_duration.as_secs_f64();
    let wall_ms = blast_duration.as_micros() as f64 / 1000.0;

    println!(
        "=== Pipelined Benchmark ({measured_orders} orders, {WARMUP_ORDERS} warmup, window={WINDOW}) ==="
    );
    println!();
    println!("  Transport: {transport_name}");
    println!();
    println!("  Throughput");
    println!("    wall time:  {wall_ms:.2} ms");
    println!(
        "    throughput: {throughput:.0} orders/sec ({:.2} µs/order)",
        1_000_000.0 / throughput
    );
    println!();
    println!("  Per-Order Round-Trip Latency");
    println!("    min:    {:>8.2} µs", histogram.min() as f64 / 1000.0);
    println!(
        "    p50:    {:>8.2} µs",
        histogram.value_at_quantile(0.50) as f64 / 1000.0
    );
    println!(
        "    p90:    {:>8.2} µs",
        histogram.value_at_quantile(0.90) as f64 / 1000.0
    );
    println!(
        "    p99:    {:>8.2} µs",
        histogram.value_at_quantile(0.99) as f64 / 1000.0
    );
    println!(
        "    p99.9:  {:>8.2} µs",
        histogram.value_at_quantile(0.999) as f64 / 1000.0
    );
    println!("    max:    {:>8.2} µs", histogram.max() as f64 / 1000.0);

    // Signal server shutdown so pipeline threads can clean up and print
    // latency-trace reports (if the feature is enabled).
    println!();
    println!("=== Pipeline Latency Trace ===");
    println!();
    shutdown.store(true, Ordering::Relaxed);
    // Give pipeline threads time to drain and print reports.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
}

/// Create a temporary directory that persists for the process lifetime.
fn tempdir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("trading-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}
