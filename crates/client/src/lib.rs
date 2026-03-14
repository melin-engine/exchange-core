//! Client library for connecting to the trading server.
//!
//! Provides a typed API over the binary wire protocol. Connects via TCP,
//! sends requests, and collects response batches using blocking I/O.

use std::io;
use std::net::SocketAddr;

use trading_protocol::blocking::{BlockingFrameReader, BlockingFrameWriter};
use trading_protocol::codec;
use trading_protocol::error::ProtocolError;
use trading_protocol::message::{Request, ResponseKind};

/// Error returned by client operations.
#[derive(Debug)]
pub enum ClientError {
    /// I/O error (connection lost, etc.).
    Io(io::Error),
    /// Protocol encoding/decoding error.
    Protocol(ProtocolError),
    /// Server closed the connection before sending BatchEnd.
    Disconnected,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::Disconnected => write!(f, "disconnected from server"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        Self::Protocol(e)
    }
}

/// Client connection to the trading server.
///
/// Sends requests and receives response batches synchronously (one
/// request at a time, blocking I/O). For pipelining, use
/// `BlockingFrameReader`/`BlockingFrameWriter` directly.
pub struct Client {
    reader: BlockingFrameReader<std::net::TcpStream>,
    writer: BlockingFrameWriter<std::net::TcpStream>,
    /// Pre-allocated encode buffer. 128 bytes is sufficient for all
    /// request types (the largest is SubmitOrder with a StopLimit at ~60 bytes).
    encode_buf: [u8; 128],
}

impl Client {
    /// Connect to a trading server at the given address.
    ///
    /// Blocks until the server sends a `ServerReady` frame, confirming that
    /// the pipeline is initialized and the connection is ready for trading.
    pub fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        let stream = std::net::TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let mut reader = BlockingFrameReader::new(stream.try_clone()?);
        let writer = BlockingFrameWriter::new(stream);

        // Wait for the ServerReady handshake before returning.
        let frame = reader.read_frame()?.ok_or(ClientError::Disconnected)?;
        let response = codec::decode_response(frame)?;
        if !matches!(response, ResponseKind::ServerReady) {
            return Err(ClientError::Protocol(
                trading_protocol::error::ProtocolError::InvalidField("expected ServerReady"),
            ));
        }

        Ok(Self {
            reader,
            writer,
            encode_buf: [0u8; 128],
        })
    }

    /// Send a request and collect all responses until BatchEnd.
    ///
    /// Returns the list of responses (excluding the BatchEnd marker itself).
    pub fn send_request(&mut self, request: &Request) -> Result<Vec<ResponseKind>, ClientError> {
        // Encode and send.
        let written = codec::encode_request(request, &mut self.encode_buf)?;
        // write_frame expects payload without length prefix; encode_request
        // writes [length(4) | tag+payload], so skip the prefix.
        self.writer.write_frame(&self.encode_buf[4..written])?;
        self.writer.flush()?;

        // Collect responses until BatchEnd. Heartbeats received during
        // idle periods are silently consumed (not part of a request batch).
        let mut responses = Vec::new();
        loop {
            let frame = self.reader.read_frame()?.ok_or(ClientError::Disconnected)?;

            let response = codec::decode_response(frame)?;
            match response {
                ResponseKind::BatchEnd => break,
                ResponseKind::Heartbeat | ResponseKind::ServerReady => continue,
                other => responses.push(other),
            }
        }

        Ok(responses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_protocol::types::{OrderId, Symbol};

    /// Send a ServerReady frame to the given writer.
    fn send_ready(writer: &mut BlockingFrameWriter<std::net::TcpStream>) {
        let mut buf = [0u8; 8];
        let written = codec::encode_response(&ResponseKind::ServerReady, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        writer.flush().unwrap();
    }

    /// Mock server that reads one request and responds with BatchEnd.
    fn mock_batch_end_server(listener: std::net::TcpListener) {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
        let mut writer = BlockingFrameWriter::new(stream);

        // Send ServerReady handshake.
        send_ready(&mut writer);

        // Read one request frame (discard it).
        let _frame = reader.read_frame().unwrap().unwrap();

        // Respond with BatchEnd.
        let mut buf = [0u8; 128];
        let written = codec::encode_response(&ResponseKind::BatchEnd, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        writer.flush().unwrap();
    }

    #[test]
    fn connect_send_receive_batch_end() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || mock_batch_end_server(listener));

        let mut client = Client::connect(addr).unwrap();
        let responses = client
            .send_request(&Request::CancelOrder {
                symbol: Symbol(1),
                order_id: OrderId(42),
            })
            .unwrap();

        // No reports before BatchEnd — just an empty batch.
        assert!(responses.is_empty());
    }

    #[test]
    fn disconnect_before_batch_end_is_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Server accepts, sends ServerReady, reads one request, then drops.
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = BlockingFrameWriter::new(stream.try_clone().unwrap());
            send_ready(&mut writer);
            let mut reader = BlockingFrameReader::new(stream);
            let _frame = reader.read_frame().unwrap();
            // Drop without sending BatchEnd.
        });

        let mut client = Client::connect(addr).unwrap();
        let result = client.send_request(&Request::CancelOrder {
            symbol: Symbol(1),
            order_id: OrderId(42),
        });

        assert!(result.is_err());
    }
}
