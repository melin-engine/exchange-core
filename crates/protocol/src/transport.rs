//! Transport abstraction layer.
//!
//! Minimal trait surface for swapping TCP / QUIC / kernel bypass later.
//! Uses RPITIT (`impl Future` in traits, stable since Rust 1.75) —
//! zero overhead, no `async_trait` macro dependency.

use std::future::Future;
use std::io;
use std::net::SocketAddr;

/// Accepts new connections. Implemented by `TcpTransportListener`, and
/// later by QUIC or kernel bypass transports.
pub trait TransportListener: Send + 'static {
    type Stream: TransportStream;
    fn accept(&mut self) -> impl Future<Output = io::Result<(Self::Stream, SocketAddr)>> + Send;
}

/// A bidirectional stream that can be split into independent read/write halves.
/// Splitting is required so the reader and writer tasks can run concurrently.
pub trait TransportStream: Send + 'static {
    type Read: TransportRead;
    type Write: TransportWrite;
    fn into_split(self) -> (Self::Read, Self::Write);
}

/// Read half of a transport stream. Delivers complete frames (length-delimited).
pub trait TransportRead: Send + 'static {
    /// Read the next complete frame. Returns `None` on clean disconnect.
    fn read_frame(&mut self) -> impl Future<Output = io::Result<Option<Vec<u8>>>> + Send;
}

/// Write half of a transport stream. Sends complete frames (length-delimited).
pub trait TransportWrite: Send + 'static {
    /// Write a complete frame (the implementation prepends the length prefix).
    fn write_frame(&mut self, data: &[u8]) -> impl Future<Output = io::Result<()>> + Send;

    /// Flush buffered data to the underlying transport.
    fn flush(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}
