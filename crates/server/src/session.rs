//! Per-connection reader thread.
//!
//! Each connection gets a dedicated OS thread that performs blocking reads
//! from the socket. Decoded requests are published directly to the input
//! disruptor via a lock-free `MultiProducer` — no mutex, no tokio.
//!
//! Writing happens in the response thread, which holds all connection
//! writers and writes directly after matching + journal gating.

use std::io::Read;

use tracing::debug;

use trading_engine::journal::event::JournalEvent;
use trading_engine::journal::pipeline::InputSlot;
use trading_engine::journal::trace::trace_ts;

use trading_disruptor::ring;

use trading_protocol::blocking::BlockingFrameReader;
use trading_protocol::codec;
use trading_protocol::message::{ConnectionId, Request};

use crate::response::ControlEvent;

/// Spawn a reader thread for a new connection.
///
/// The reader performs blocking I/O on its own OS thread — no tokio
/// scheduling jitter. On disconnect, sends `ControlEvent::Disconnected`
/// to the response thread.
pub fn spawn_reader_thread<R: Read + Send + 'static>(
    connection_id: ConnectionId,
    reader: R,
    producer: ring::MultiProducer<InputSlot>,
    control_tx: std::sync::mpsc::Sender<ControlEvent>,
    addr: std::net::SocketAddr,
) {
    std::thread::Builder::new()
        .name(format!("reader-{}", connection_id.0))
        .spawn(move || {
            reader_loop(connection_id, reader, producer, &control_tx, addr);

            // Notify response thread to remove this connection's writer.
            let _ = control_tx.send(ControlEvent::Disconnected {
                connection_id: connection_id.0,
            });
        })
        .expect("failed to spawn reader thread");
}

/// Blocking reader loop. Reads frames, decodes, publishes to the disruptor.
fn reader_loop<R: Read>(
    connection_id: ConnectionId,
    reader: R,
    producer: ring::MultiProducer<InputSlot>,
    _control_tx: &std::sync::mpsc::Sender<ControlEvent>,
    addr: std::net::SocketAddr,
) {
    let mut frame_reader = BlockingFrameReader::new(reader);

    #[cfg(feature = "latency-trace")]
    let mut publish_hist = trading_engine::journal::trace::StageHistogram::new(
        "reader: publish (decode → disruptor publish)",
    );

    loop {
        let frame = match frame_reader.read_frame() {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                debug!(addr = %addr, "client disconnected");
                break;
            }
            Err(e) => {
                debug!(addr = %addr, error = %e, "read error");
                break;
            }
        };

        let request = match codec::decode_request(&frame) {
            Ok(req) => req,
            Err(e) => {
                debug!(addr = %addr, error = %e, "decode error");
                continue;
            }
        };

        #[allow(clippy::let_unit_value)] // ZST when latency-trace is disabled
        let recv_ts = trace_ts();

        let event = request_to_event(&request);

        #[cfg(feature = "latency-trace")]
        let pre_publish = trace_ts();

        // Lock-free publish to the disruptor. MultiProducer uses CAS-based
        // slot claiming — no mutex, scales to any connection count.
        producer.publish(InputSlot {
            connection_id: connection_id.0,
            event,
            publish_ts: trace_ts(),
            recv_ts,
        });

        #[cfg(feature = "latency-trace")]
        publish_hist.record_ns(trading_engine::journal::trace::trace_elapsed_ns(
            pre_publish,
            trace_ts(),
        ));
    }

    #[cfg(feature = "latency-trace")]
    publish_hist.print_report();
}

/// Convert a wire `Request` to a `JournalEvent` for the pipeline.
fn request_to_event(request: &Request) -> JournalEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => JournalEvent::SubmitOrder { symbol, order },
        Request::CancelOrder { symbol, order_id } => JournalEvent::CancelOrder { symbol, order_id },
    }
}
