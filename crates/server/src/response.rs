//! Response stage — routes matching output directly to connection sockets.
//!
//! Consumes from the output SPSC queue (matching → response) and writes
//! encoded responses directly to each connection's blocking socket writer.
//! Before sending, waits for the journal cursor to confirm durability —
//! this is the persist-before-ack boundary.
//!
//! Runs on a dedicated OS thread. No tokio involvement — eliminates
//! async scheduling jitter from the response path.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

use trading_disruptor::padding::Sequence;
use trading_disruptor::spsc;

use trading_engine::journal::pipeline::{OutputPayload, OutputSlot};
#[cfg(feature = "latency-trace")]
use trading_engine::journal::trace;

use trading_protocol::blocking::BlockingFrameWriter;
use trading_protocol::codec;
use trading_protocol::message::ResponseKind;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum encoded response size. Responses are small (execution reports),
/// so 128 bytes is generous.
const MAX_RESPONSE_BUF: usize = 128;

/// Control plane events for connection registration.
///
/// Sent on a `std::sync::mpsc` channel (not the disruptor) because
/// connect/disconnect is rare and not on the hot path.
///
/// Uses `Box<dyn Write + Send>` to erase the concrete stream type
/// (TCP or UDS). The vtable dispatch cost is negligible compared to
/// the syscall cost of write_all.
pub enum ControlEvent {
    /// Register a new connection's blocking writer.
    Connected {
        connection_id: u64,
        writer: BlockingFrameWriter<Box<dyn Write + Send>>,
    },
    /// Remove a disconnected connection's writer.
    Disconnected { connection_id: u64 },
}

/// Run the response stage loop. Blocks the calling thread until shutdown.
///
/// Consumes from the output SPSC and writes encoded responses directly
/// to each connection's socket. For each output slot, waits until the
/// journal cursor has advanced past `input_seq` before writing — ensuring
/// the client never receives a response for an event that isn't yet durable.
pub fn run(
    mut consumer: spsc::Consumer<OutputSlot>,
    control_rx: mpsc::Receiver<ControlEvent>,
    journal_cursor: Arc<Sequence>,
    shutdown: &AtomicBool,
) {
    // Connection table: maps connection IDs to their blocking writers.
    // HashMap for O(1) lookup. Connection count bounded by OS fd limits.
    let mut connections: HashMap<u64, BlockingFrameWriter<Box<dyn Write + Send>>> = HashMap::new();

    let mut batch = [OutputSlot::default(); MAX_BATCH];
    let mut encode_buf = [0u8; MAX_RESPONSE_BUF];

    // Cached journal cursor value to avoid atomic reads on every slot.
    #[cfg(not(feature = "no-fsync"))]
    let mut cached_journal_pos: u64 = 0;
    // Suppress unused warnings when journal gating is disabled.
    #[cfg(feature = "no-fsync")]
    let _ = &journal_cursor;

    #[cfg(feature = "latency-trace")]
    let mut spsc_hist =
        trace::StageHistogram::new("response: SPSC wakeup (matching publish → response consume)");
    #[cfg(feature = "latency-trace")]
    let mut dispatch_hist =
        trace::StageHistogram::new("response: dispatch (consume → socket write)");
    #[cfg(feature = "latency-trace")]
    let mut server_e2e_hist =
        trace::StageHistogram::new("server e2e (reader recv → response flush)");

    loop {
        if shutdown.load(Ordering::Relaxed) {
            #[cfg(feature = "latency-trace")]
            {
                spsc_hist.print_report();
                dispatch_hist.print_report();
                server_e2e_hist.print_report();
            }
            return;
        }

        // Poll control channel (non-blocking) for connect/disconnect.
        while let Ok(event) = control_rx.try_recv() {
            match event {
                ControlEvent::Connected {
                    connection_id,
                    writer,
                } => {
                    connections.insert(connection_id, writer);
                }
                ControlEvent::Disconnected { connection_id } => {
                    connections.remove(&connection_id);
                }
            }
        }

        // Consume output slots from matching stage.
        let count = consumer.consume_batch(&mut batch, MAX_BATCH);
        if count == 0 {
            std::hint::spin_loop();
            continue;
        }

        #[cfg(feature = "latency-trace")]
        let consume_ts = trace::trace_ts();

        for slot in &batch[..count] {
            #[cfg(feature = "latency-trace")]
            spsc_hist.record_ns(trace::trace_elapsed_ns(slot.match_complete_ts, consume_ts));

            // Wait for the journal to confirm this event is durable.
            #[cfg(not(feature = "no-fsync"))]
            {
                let needed = slot.input_seq + 1;
                if cached_journal_pos < needed {
                    loop {
                        cached_journal_pos = journal_cursor.get().load(Ordering::Acquire);
                        if cached_journal_pos >= needed {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            }

            let kind = match slot.payload {
                OutputPayload::Report(report) => ResponseKind::Report(report),
                OutputPayload::BatchEnd => ResponseKind::BatchEnd,
                OutputPayload::EngineError => ResponseKind::EngineError,
            };

            let is_batch_end = matches!(kind, ResponseKind::BatchEnd);

            if let Some(writer) = connections.get_mut(&slot.connection_id) {
                // Encode the response directly to wire format.
                let written = match codec::encode_response(&kind, &mut encode_buf) {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::error!(
                            connection_id = slot.connection_id,
                            error = %e,
                            "encode error"
                        );
                        continue;
                    }
                };

                // write_frame expects the payload (tag + fields), not the
                // length prefix. encode_response writes [length(4) | tag+payload].
                if let Err(e) = writer.write_frame(&encode_buf[4..written]) {
                    tracing::debug!(
                        connection_id = slot.connection_id,
                        error = %e,
                        "write error, dropping connection"
                    );
                    connections.remove(&slot.connection_id);
                    continue;
                }

                // Flush after each batch to minimize latency — the client
                // is waiting for all reports before proceeding.
                if is_batch_end && let Err(e) = writer.flush() {
                    tracing::debug!(
                        connection_id = slot.connection_id,
                        error = %e,
                        "flush error, dropping connection"
                    );
                    connections.remove(&slot.connection_id);
                    continue;
                }

                // Record server-side end-to-end: reader recv → response flush.
                #[cfg(feature = "latency-trace")]
                if is_batch_end {
                    server_e2e_hist
                        .record_ns(trace::trace_elapsed_ns(slot.recv_ts, trace::trace_ts()));
                }
            }
        }

        #[cfg(feature = "latency-trace")]
        dispatch_hist.record_ns(trace::trace_elapsed_ns(consume_ts, trace::trace_ts()));
    }
}
