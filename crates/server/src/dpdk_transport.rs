//! DPDK transport integration — single poll thread for NIC I/O + TCP.
//!
//! Replaces both the epoll reader pool and the response stage's socket
//! writes. A single DPDK poll thread owns all NIC I/O:
//!
//! - **Inbound**: `rx_burst` → smoltcp → frame decode → disruptor publish
//! - **Outbound**: response SPSC → per-connection TX queue → smoltcp → `tx_burst`
//!
//! The response stage still runs on its own pinned thread for cursor
//! gating and encoding, but instead of calling `write_all` on kernel
//! sockets, it pushes encoded frames into a lock-free SPSC queue per
//! connection. The DPDK poll thread drains these into smoltcp sockets.
//!
//! # Thread model
//!
//! ```text
//! Core N:   DPDK poll thread  (rx_burst, smoltcp, frame decode, tx_burst)
//! Core 1:   Journal stage     (unchanged)
//! Core 2:   Matching stage    (unchanged)
//! Core 3:   Response stage    (encodes to SPSC queues instead of kernel sockets)
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

use melin_disruptor::ring;
use melin_dpdk::transport::DpdkTransport;
use melin_engine::journal::event::JournalEvent;
use melin_engine::journal::pipeline::InputSlot;
use melin_engine::journal::trace::trace_ts;
use melin_protocol::auth::Permission;
use melin_protocol::codec;
use melin_protocol::message::{ConnectionId, Request};
use smoltcp::iface::SocketHandle;
use tracing::debug;

use crate::dpdk_response::{ControlEvent, TxFrame};

/// Maximum frame payload size (matches epoll reader).
const MAX_FRAME_SIZE: usize = 1024;

/// Per-connection state in the DPDK poll thread.
struct ConnectionState {
    connection_id: ConnectionId,
    addr: SocketAddr,
    handle: SocketHandle,
    permission: Permission,
    /// Incremental frame parsing state: accumulates bytes until a
    /// complete length-prefixed frame is available.
    parse_buf: Vec<u8>,
}

/// Run the DPDK poll loop.
///
/// This replaces the epoll reader pool. It accepts connections, parses
/// frames, publishes events to the disruptor, and drains the TX channel
/// from the response stage into smoltcp sockets.
///
/// Called from a dedicated OS thread pinned to its own core.
pub fn run_dpdk_poll(
    mut transport: DpdkTransport,
    producer: ring::MultiProducer<InputSlot>,
    control_tx: mpsc::Sender<ControlEvent>,
    tx_rx: mpsc::Receiver<TxFrame>,
    shutdown: &AtomicBool,
) {
    // Map from smoltcp SocketHandle index → connection state.
    let mut connections: HashMap<usize, ConnectionState> = HashMap::with_capacity(256);
    // Reverse map: connection_id → socket handle index (for TX routing).
    let mut id_to_handle: HashMap<u64, usize> = HashMap::with_capacity(256);
    let mut next_connection_id: u64 = 1;

    // Scratch buffer for reading from smoltcp sockets.
    let mut read_buf = [0u8; MAX_FRAME_SIZE + 4];

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // 1. Poll NIC + smoltcp.
        transport.poll();

        // 2. Accept new connections.
        for accepted in transport.take_accepted() {
            let conn_id = ConnectionId(next_connection_id);
            next_connection_id += 1;

            debug!(
                connection_id = conn_id.0,
                peer = %accepted.peer,
                "DPDK: new connection"
            );

            let handle_idx: usize = accepted.handle.into();
            connections.insert(
                handle_idx,
                ConnectionState {
                    connection_id: conn_id,
                    addr: accepted.peer,
                    handle: accepted.handle,
                    permission: Permission::Trader, // TODO: auth handshake
                    parse_buf: Vec::with_capacity(MAX_FRAME_SIZE + 4),
                },
            );
            id_to_handle.insert(conn_id.0, handle_idx);

            // Notify the response stage about the new connection.
            let _ = control_tx.send(ControlEvent::Connected {
                connection_id: conn_id.0,
            });
        }

        // 3. Drain TX frames from the response stage into smoltcp sockets.
        while let Ok(frame) = tx_rx.try_recv() {
            if let Some(&handle_idx) = id_to_handle.get(&frame.connection_id) {
                if let Some(conn) = connections.get(&handle_idx) {
                    transport.queue_send(conn.handle, &frame.data);
                }
            }
        }

        // 4. Read data from all active connections.
        // Collect handles to avoid borrow conflict with `transport`.
        let handle_indices: Vec<usize> = connections.keys().copied().collect();

        for handle_idx in handle_indices {
            let conn = match connections.get_mut(&handle_idx) {
                Some(c) => c,
                None => continue,
            };

            // Read available data from smoltcp socket.
            let n = transport.recv(conn.handle, &mut read_buf);
            if n == 0 {
                // Check if connection was closed.
                if !transport.is_active(conn.handle) {
                    debug!(
                        connection_id = conn.connection_id.0,
                        addr = %conn.addr,
                        "DPDK: connection closed"
                    );
                    let _ = control_tx.send(ControlEvent::Disconnected {
                        connection_id: conn.connection_id.0,
                    });
                    id_to_handle.remove(&conn.connection_id.0);
                    connections.remove(&handle_idx);
                }
                continue;
            }

            // Append to parse buffer and try to extract frames.
            conn.parse_buf.extend_from_slice(&read_buf[..n]);

            // Parse length-prefixed frames: [u32 length][payload].
            while conn.parse_buf.len() >= 4 {
                let frame_len = u32::from_le_bytes([
                    conn.parse_buf[0],
                    conn.parse_buf[1],
                    conn.parse_buf[2],
                    conn.parse_buf[3],
                ]) as usize;

                if frame_len > MAX_FRAME_SIZE {
                    debug!(
                        connection_id = conn.connection_id.0,
                        frame_len, "DPDK: oversized frame, dropping connection"
                    );
                    transport.close(conn.handle);
                    let _ = control_tx.send(ControlEvent::Disconnected {
                        connection_id: conn.connection_id.0,
                    });
                    id_to_handle.remove(&conn.connection_id.0);
                    connections.remove(&handle_idx);
                    break;
                }

                if conn.parse_buf.len() < 4 + frame_len {
                    // Incomplete frame — wait for more data.
                    break;
                }

                // Extract the frame payload.
                let payload = &conn.parse_buf[4..4 + frame_len];

                // Decode the request.
                match codec::decode_request(payload) {
                    Ok(request) => {
                        // Filter heartbeats and auth — not pipeline events.
                        if matches!(
                            request,
                            Request::Heartbeat | Request::ChallengeResponse { .. }
                        ) {
                            // Heartbeat: no-op (connection timeout is
                            // handled by smoltcp TCP keepalive).
                        } else {
                            let recv_ts = trace_ts();
                            let event = request_to_event(&request);
                            producer.publish(InputSlot {
                                connection_id: conn.connection_id.0,
                                event,
                                publish_ts: trace_ts(),
                                recv_ts,
                            });
                        }
                    }
                    Err(e) => {
                        debug!(
                            connection_id = conn.connection_id.0,
                            error = %e,
                            "DPDK: decode error"
                        );
                    }
                }

                // Remove the consumed frame from the parse buffer.
                conn.parse_buf.drain(..4 + frame_len);
            }
        }
    }
}

/// Convert a decoded `Request` to a `JournalEvent`.
/// Mirrors the epoll reader's `request_to_event` — all variants are
/// mapped 1:1 except heartbeats/auth (filtered by the caller).
fn request_to_event(request: &Request) -> JournalEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => JournalEvent::SubmitOrder { symbol, order },
        Request::CancelOrder {
            symbol,
            account,
            order_id,
        } => JournalEvent::CancelOrder {
            symbol,
            account,
            order_id,
        },
        Request::CancelAll { account } => JournalEvent::CancelAll { account },
        Request::AddInstrument { spec } => JournalEvent::AddInstrument { spec },
        Request::Deposit {
            account,
            currency,
            amount,
        } => JournalEvent::Deposit {
            account,
            currency,
            amount,
        },
        Request::SetRiskLimits { symbol, limits } => JournalEvent::SetRiskLimits { symbol, limits },
        Request::SetCircuitBreaker { symbol, config } => {
            JournalEvent::SetCircuitBreaker { symbol, config }
        }
        Request::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => JournalEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        },
        Request::SetFeeSchedule { symbol, schedule } => {
            JournalEvent::SetFeeSchedule { symbol, schedule }
        }
        Request::QueryStats => JournalEvent::QueryStats,
        Request::Heartbeat | Request::ChallengeResponse { .. } => {
            unreachable!("heartbeats and auth filtered before request_to_event")
        }
    }
}
