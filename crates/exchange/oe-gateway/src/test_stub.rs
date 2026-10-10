//! Loopback Melin server stub for end-to-end gateway tests.
//!
//! Accepts one TCP connection, runs the challenge/response handshake
//! (trusting any signature), and then acts as a request/response
//! playback surface driven by the test via channels.
//!
//! Lifetime:
//! ```text
//!   test                 stub thread            gateway
//!     |                      |                     |
//!     |---- MelinStub::start-|                     |
//!     |    bind 127.0.0.1:0  |                     |
//!     |<---- port ------------|                     |
//!     | (test builds config & spawns gateway)      |
//!     |                      |<-- accept connect --|
//!     |                      |--- Challenge ------>|
//!     |                      |<-- ChallengeResp ---|
//!     |                      |--- ServerReady ---->|
//!     |<-- next_request -----|                     |
//!     |---- send_response ->-|                     |
//!     |                      |--- Response ------->|
//!     |    ...               |                     |
//!     |---- drop() ----------|                     |
//!     |    (joins thread)                          |
//! ```

#![cfg(test)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use melin_client::framing::FrameDecoder;
use melin_ec_protocol::codec;
use melin_ec_protocol::message::{Request, ResponseKind};
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;

/// Control handle owned by the test. Starts a stub listener, connects
/// to the first gateway connection, and exposes channels for driving
/// the request/response flow.
pub struct MelinStub {
    port: u16,
    requests: Receiver<(u64, Request)>,
    /// Encoded frames for the stub to write, each `Vec` in one write so
    /// a test controls which frames share a TCP segment.
    responses: Sender<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
    /// Set by the stub thread when it observes EOF on the gateway
    /// connection (i.e. the gateway closed its Melin socket).
    disconnected: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    /// Errors observed by the stub thread — pulled in `drop` to fail
    /// the test if the stub crashed.
    errors: Arc<Mutex<Vec<String>>>,
}

impl MelinStub {
    /// Bind a listener on `127.0.0.1:0`, spawn the stub thread, and
    /// return a handle. The thread blocks waiting for one inbound
    /// connection (the gateway).
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let port = listener.local_addr().unwrap().port();

        let (req_tx, req_rx) = channel::<(u64, Request)>();
        let (resp_tx, resp_rx) = channel::<Vec<u8>>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let disconnected = Arc::new(AtomicBool::new(false));
        let disconnected_clone = disconnected.clone();
        let errors = Arc::new(Mutex::new(Vec::<String>::new()));
        let errors_clone = errors.clone();

        let join = std::thread::spawn(move || {
            if let Err(e) = run_stub(
                listener,
                req_tx,
                resp_rx,
                shutdown_clone,
                disconnected_clone,
            ) {
                errors_clone.lock().unwrap().push(e);
            }
        });

        Self {
            port,
            requests: req_rx,
            responses: resp_tx,
            shutdown,
            disconnected,
            join: Some(join),
            errors,
        }
    }

    /// Wait up to `timeout` for the stub to observe the gateway
    /// closing its Melin socket. Returns true if EOF was seen.
    pub fn wait_for_disconnect(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.disconnected.load(Ordering::Relaxed) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.disconnected.load(Ordering::Relaxed)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Wait up to `timeout` for the next request from the gateway.
    /// Panics if the timeout expires — tests should set this generously
    /// enough to absorb scheduling jitter but short enough to fail fast.
    pub fn next_request(&self, timeout: Duration) -> (u64, Request) {
        self.requests
            .recv_timeout(timeout)
            .expect("stub did not receive a request in time")
    }

    /// Queue a response for the stub to send on the wire. Non-blocking.
    pub fn send_response(&self, resp: ResponseKind) {
        self.send_bytes(encode_response(&resp));
    }

    /// Queue a heartbeat and then a response, written together, so they
    /// reach the gateway in the same receive.
    pub fn send_response_behind_heartbeat(&self, resp: ResponseKind) {
        let mut bytes = encode_transport(&TransportResponse::Heartbeat);
        bytes.extend_from_slice(&encode_response(&resp));
        self.send_bytes(bytes);
    }

    fn send_bytes(&self, bytes: Vec<u8>) {
        self.responses
            .send(bytes)
            .expect("stub thread dropped response channel");
    }
}

impl Drop for MelinStub {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Wake the stub thread if it's blocked in accept by dialing it.
        // (If it already accepted, this connect just gets dropped.)
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
        let errs = self.errors.lock().unwrap();
        if !errs.is_empty() && !std::thread::panicking() {
            panic!("stub thread errors: {:?}", *errs);
        }
    }
}

/// Stub thread main loop. Returns Err with a message if anything
/// unexpected happens — the test Drop surface will then fail.
fn run_stub(
    listener: TcpListener,
    requests: Sender<(u64, Request)>,
    responses: Receiver<Vec<u8>>,
    shutdown: Arc<AtomicBool>,
    disconnected: Arc<AtomicBool>,
) -> Result<(), String> {
    // Only accept the first real inbound connection. If shutdown fires
    // before the gateway dials, we bail out cleanly via the dummy dial
    // the handle does on Drop.
    listener
        .set_nonblocking(false)
        .map_err(|e| format!("set_nonblocking: {e}"))?;
    let (mut stream, _peer) = listener.accept().map_err(|e| format!("accept: {e}"))?;
    if shutdown.load(Ordering::Relaxed) {
        return Ok(());
    }

    // Short read timeout so the loop can poll the response channel and
    // the shutdown flag. 50ms is well below any test assertion timeout
    // but keeps the loop responsive.
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .map_err(|e| format!("set_read_timeout: {e}"))?;

    // One decoder for the whole connection: the gateway's frames may
    // arrive split or batched however TCP delivers them, handshake and
    // requests alike.
    let mut decoder = FrameDecoder::new();

    // --- Auth handshake ---
    // Send Challenge with a deterministic nonce. We don't verify the
    // signature the gateway returns — tests only care that the state
    // machine progresses.
    let nonce = [0u8; 32];
    write_transport(&mut stream, &TransportResponse::Challenge { nonce })?;

    let payload = read_frame_blocking(&mut stream, &mut decoder, &shutdown)?;
    control_codec::decode_challenge_response(&payload)
        .map_err(|e| format!("expected ChallengeResponse, got {e:?}"))?;
    write_transport(&mut stream, &TransportResponse::ServerReady)?;

    // Production-side, the gateway issues `QueryRequestSeq` immediately
    // after `ServerReady` to learn the engine's per-key request_seq HWM
    // before unblocking the FIX session (otherwise a fresh client
    // process re-uses seqs the engine has already accepted and every
    // order is rejected as `DuplicateRequest`). The stub mirrors that
    // contract: read the query, return `hwm = 0` (the stub holds no
    // engine-side state) followed by `BatchEnd` per the query batch
    // shape, then enter the regular request loop. Tests don't see this
    // exchange — it stays inside the stub.
    let payload = read_frame_blocking(&mut stream, &mut decoder, &shutdown)?;
    match codec::decode_request(&payload).map_err(|e| format!("decode_request: {e:?}"))? {
        (_, Request::QueryRequestSeq) => {}
        (_, other) => return Err(format!("expected QueryRequestSeq, got {other:?}")),
    }
    write_bytes(
        &mut stream,
        &encode_response(&ResponseKind::RequestSeqHwm { hwm: 0 }),
    )?;
    write_bytes(&mut stream, &encode_transport(&TransportResponse::BatchEnd))?;

    // --- Request/response loop ---
    let mut tmp = [0u8; 256];
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Drain any pending responses first.
        loop {
            match responses.try_recv() {
                Ok(bytes) => write_bytes(&mut stream, &bytes)?,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

        // Try to read. 50ms timeout means we wake often enough to
        // notice new queued responses and the shutdown flag.
        match stream.read(&mut tmp) {
            Ok(0) => {
                // Gateway closed.
                disconnected.store(true, Ordering::Relaxed);
                return Ok(());
            }
            Ok(n) => decoder.push(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                disconnected.store(true, Ordering::Relaxed);
                return Ok(());
            }
            Err(e) => return Err(format!("read: {e}")),
        }

        // Decode as many complete requests as have arrived.
        while let Some(payload) = decoder.next().map_err(|e| e.to_string())? {
            let request =
                codec::decode_request(payload).map_err(|e| format!("decode_request: {e:?}"))?;
            requests
                .send(request)
                .map_err(|_| "request channel closed".to_string())?;
        }
    }
    Ok(())
}

/// Blocking single-frame read used during the handshake, honoring the
/// shutdown flag between short read intervals. The frame's payload,
/// without the length prefix.
fn read_frame_blocking(
    stream: &mut TcpStream,
    decoder: &mut FrameDecoder,
    shutdown: &Arc<AtomicBool>,
) -> Result<Vec<u8>, String> {
    let mut tmp = [0u8; 128];
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Err("shutdown during handshake".to_string());
        }
        if let Some(payload) = decoder.next().map_err(|e| e.to_string())? {
            return Ok(payload.to_vec());
        }
        match stream.read(&mut tmp) {
            Ok(0) => return Err("EOF during handshake".to_string()),
            Ok(n) => decoder.push(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => return Err(format!("handshake read: {e}")),
        }
    }
}

/// One of the transport's own frames, as the node's runtime sends the
/// handshake, heartbeats and batch ends.
fn encode_transport(resp: &TransportResponse) -> Vec<u8> {
    let mut buf = [0u8; 64];
    let n = control_codec::encode_transport_response(resp, &mut buf)
        .expect("a transport frame fits 64 bytes");
    buf[..n].to_vec()
}

/// One application response frame.
fn encode_response(resp: &ResponseKind) -> Vec<u8> {
    let mut buf = [0u8; 256];
    let n = codec::encode_response(resp, &mut buf).expect("a response frame fits 256 bytes");
    buf[..n].to_vec()
}

fn write_transport(stream: &mut TcpStream, resp: &TransportResponse) -> Result<(), String> {
    write_bytes(stream, &encode_transport(resp))
}

fn write_bytes(stream: &mut TcpStream, bytes: &[u8]) -> Result<(), String> {
    stream.write_all(bytes).map_err(|e| format!("write: {e}"))
}
