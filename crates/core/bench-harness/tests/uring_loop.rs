//! End-to-end test of the io_uring loop against a trivial echo server.
//!
//! Covers what unit tests on the individual pieces cannot: that framing,
//! the send window, in-flight accounting, phase classification, and the
//! [`Workload`] seam agree with each other over a real socket.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use melin_bench_harness::pacing::PaceStats;
use melin_bench_harness::phases::BenchPhases;
use melin_bench_harness::uring::{Connection, LoopConfig, run_loop};
use melin_bench_harness::workload::{Outcomes, Workload};

/// Response byte meaning "this completes one request". Any other value is
/// an unsolicited frame the loop must count but not time.
const COMPLETE: u8 = 1;
const CHATTER: u8 = 2;

#[derive(Default)]
struct Counts {
    completions: u64,
    chatter: u64,
}

impl Outcomes for Counts {
    fn merge(&mut self, other: &Self) {
        self.completions += other.completions;
        self.chatter += other.chatter;
    }
}

/// Sends a fixed 4-byte payload and treats [`COMPLETE`] as the completion
/// marker.
struct Echo {
    outcomes: Counts,
    sent: u64,
}

impl Workload for Echo {
    type Response = u8;
    type Outcomes = Counts;

    fn next_frame(&mut self, out: &mut Vec<u8>) {
        out.extend_from_slice(&4u32.to_le_bytes());
        out.extend_from_slice(b"ping");
        self.sent += 1;
    }

    fn decode(&self, frame: &[u8]) -> u8 {
        assert_eq!(frame.len(), 1, "server sends single-byte bodies");
        frame[0]
    }

    fn completes_request(&self, response: &u8) -> bool {
        *response == COMPLETE
    }

    fn record(&mut self, response: &u8) {
        match *response {
            COMPLETE => self.outcomes.completions += 1,
            _ => self.outcomes.chatter += 1,
        }
    }

    fn outcomes(&self) -> &Counts {
        &self.outcomes
    }
}

/// Serve one connection: for every request frame, reply with one chatter
/// frame followed by one completion frame. The extra frame is the point —
/// it proves the loop times completions only, and that two frames landing
/// in a single RECV are parsed as two.
fn serve(mut stream: TcpStream, stop: Arc<std::sync::atomic::AtomicBool>) {
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .expect("set read timeout");
    let mut buf = [0u8; 4096];
    let mut pending = Vec::new();
    while !stop.load(Ordering::Relaxed) {
        let n = match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => n,
            // Timeouts are expected: they are how this loop notices `stop`.
            Err(_) => continue,
        };
        pending.extend_from_slice(&buf[..n]);

        let mut cursor = 0;
        let mut out = Vec::new();
        while cursor + 4 <= pending.len() {
            let len = u32::from_le_bytes(pending[cursor..cursor + 4].try_into().unwrap()) as usize;
            if cursor + 4 + len > pending.len() {
                break;
            }
            cursor += 4 + len;
            for byte in [CHATTER, COMPLETE] {
                out.extend_from_slice(&1u32.to_le_bytes());
                out.push(byte);
            }
        }
        pending.drain(..cursor);
        if !out.is_empty() && stream.write_all(&out).is_err() {
            return;
        }
    }
}

/// Every completion is timed exactly once, unsolicited frames are counted
/// but never timed, and the measured phase produces samples.
#[test]
fn loop_times_completions_and_counts_every_frame() {
    const CONNS: usize = 2;
    const WINDOW: usize = 8;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let server_stop = Arc::clone(&stop);
    let server = std::thread::spawn(move || {
        let mut handles = Vec::new();
        for _ in 0..CONNS {
            let (stream, _) = listener.accept().expect("accept");
            let s = Arc::clone(&server_stop);
            handles.push(std::thread::spawn(move || serve(stream, s)));
        }
        for h in handles {
            h.join().expect("server conn thread");
        }
    });

    let connections: Vec<Connection<Echo>> = (0..CONNS)
        .map(|_| {
            let stream = TcpStream::connect(addr).expect("connect");
            let write_half = stream.try_clone().expect("clone stream");
            Connection::new(
                stream,
                write_half,
                Echo {
                    outcomes: Counts::default(),
                    sent: 0,
                },
                WINDOW,
            )
        })
        .collect();

    let phases = BenchPhases {
        warmup: Duration::from_millis(50),
        measured: Duration::from_millis(200),
        cooldown: Duration::from_millis(50),
    };
    let start = Instant::now();
    let progress = Arc::new(AtomicU64::new(0));
    let result = run_loop(
        connections,
        LoopConfig {
            window: WINDOW,
            bench_start: start,
            deadlines: phases.deadlines(start),
            phases,
            progress: Arc::clone(&progress),
            target_rate: 0,
            total_clients: CONNS,
            thread_idx: 0,
            total_threads: 1,
            pace_stats: Arc::new(PaceStats::default()),
        },
    );

    stop.store(true, Ordering::Relaxed);
    server.join().expect("server thread");

    assert!(
        !result.histogram.is_empty(),
        "measured phase recorded no latency samples"
    );
    assert!(
        result.measured_start.is_some(),
        "measured_start must be set once a sample lands"
    );
    // The histogram covers the measured phase only; outcomes cover every
    // phase, so completions is the larger of the two.
    assert!(
        result.outcomes.completions >= result.histogram.len(),
        "completions {} < timed samples {}",
        result.outcomes.completions,
        result.histogram.len()
    );
    // The server emits exactly one chatter frame per completion frame.
    assert_eq!(
        result.outcomes.chatter, result.outcomes.completions,
        "chatter frames must be counted but never timed"
    );
    assert_eq!(
        progress.load(Ordering::Relaxed),
        result.histogram.len(),
        "progress counter must match the measured sample count"
    );
}
