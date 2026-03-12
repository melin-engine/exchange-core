//! Publisher thread — bridges tokio async world to the disruptor pipeline.
//!
//! Receives `EngineCommand` messages from session reader tasks via a tokio
//! mpsc channel, then either:
//! - **Request**: publishes to the input disruptor ring buffer (hot path).
//! - **Connected/Disconnected**: forwards to the response stage's control
//!   channel (rare, not hot path).

use std::sync::mpsc;

use tokio::sync::mpsc as tokio_mpsc;
use tracing::info;

use trading_engine::journal::event::JournalEvent;
use trading_engine::journal::pipeline::InputSlot;
#[cfg(feature = "latency-trace")]
use trading_engine::journal::trace;
use trading_engine::journal::trace::trace_ts;

use trading_disruptor::ring;

use trading_protocol::message::{EngineCommand, Request};

use crate::response::ControlEvent;

/// Run the publisher loop. Blocks the calling thread until the command
/// channel is closed (server shutdown).
///
/// This thread is the single writer to the input disruptor — consistent
/// with the LMAX single-writer principle.
pub fn run(
    mut rx: tokio_mpsc::Receiver<EngineCommand>,
    mut input_producer: ring::Producer<InputSlot>,
    control_tx: mpsc::Sender<ControlEvent>,
) {
    #[cfg(feature = "latency-trace")]
    let mut mpsc_hist =
        trace::StageHistogram::new("publisher: tokio mpsc (reader send → publisher recv)");

    loop {
        let cmd = match rx.blocking_recv() {
            Some(cmd) => cmd,
            None => {
                // All senders dropped — server is shutting down.
                info!("command channel closed, shutting down publisher");
                break;
            }
        };

        match cmd {
            EngineCommand::Connected {
                connection_id,
                sender,
            } => {
                // Route to the response stage via the control channel.
                let _ = control_tx.send(ControlEvent::Connected {
                    connection_id: connection_id.0,
                    sender,
                });
            }
            EngineCommand::Disconnected { connection_id } => {
                let _ = control_tx.send(ControlEvent::Disconnected {
                    connection_id: connection_id.0,
                });
            }
            EngineCommand::Request {
                connection_id,
                request,
                sent_ts,
            } => {
                #[cfg(feature = "latency-trace")]
                mpsc_hist.record_ns(trace::trace_elapsed_ns(sent_ts, trace_ts()));

                let event = request_to_event(&request);
                input_producer.publish(InputSlot {
                    connection_id: connection_id.0,
                    event,
                    publish_ts: trace_ts(),
                    recv_ts: sent_ts,
                });
            }
        }
    }

    #[cfg(feature = "latency-trace")]
    mpsc_hist.print_report();
}

/// Convert a wire `Request` to a `JournalEvent` for the pipeline.
fn request_to_event(request: &Request) -> JournalEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => JournalEvent::SubmitOrder { symbol, order },
        Request::CancelOrder { symbol, order_id } => JournalEvent::CancelOrder { symbol, order_id },
    }
}
