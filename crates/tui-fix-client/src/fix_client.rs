//! Blocking FIX 4.4 TCP client for the TUI.
//!
//! Manages a single FIX session: Logon, message send/receive,
//! sequence numbering, and heartbeat. Used by the TUI to connect
//! to both the oe-gateway and md-gateway.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use melin_gateway_core::fix::parse::{self, FixMessage};
use melin_gateway_core::fix::serialize::FixMessageBuilder;
use melin_gateway_core::fix::tags;

/// A blocking FIX 4.4 session client.
pub struct FixClient {
    stream: TcpStream,
    sender_comp_id: String,
    target_comp_id: String,
    outbound_seq: u64,
    /// Accumulates partial reads until a complete FIX message arrives.
    parse_buf: Vec<u8>,
    /// Backing buffer for the most recently returned message. Owned by
    /// the client and re-used across `recv`/`try_recv` calls so that the
    /// returned `FixMessage<'_>` can borrow without leaking. Reusing the
    /// allocation also avoids a per-message malloc on the bot's hot path.
    recv_buf: Vec<u8>,
}

impl FixClient {
    /// Connect to a FIX gateway and perform the Logon handshake.
    ///
    /// `addr` is resolved via DNS so hostnames like "localhost:9000" work.
    /// Blocks until the Logon response is received or the timeout expires.
    pub fn connect(
        addr: &str,
        sender_comp_id: &str,
        target_comp_id: &str,
        heartbeat_secs: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use std::net::ToSocketAddrs;
        let sock = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| format!("no address resolved for {addr}"))?;
        let stream = TcpStream::connect_timeout(&sock, Duration::from_secs(5))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_nodelay(true)?;

        let mut client = Self {
            stream,
            sender_comp_id: sender_comp_id.to_string(),
            target_comp_id: target_comp_id.to_string(),
            outbound_seq: 1,
            parse_buf: Vec::with_capacity(4096),
            recv_buf: Vec::with_capacity(512),
        };

        // Send Logon.
        let logon = FixMessageBuilder::new(tags::MSG_LOGON)
            .str_tag(tags::ENCRYPT_METHOD, "0")
            .u64_tag(tags::HEART_BT_INT, heartbeat_secs);
        client.send_builder(logon)?;

        // Wait for Logon response.
        let response = client.recv()?;
        if response.msg_type() != tags::MSG_LOGON {
            return Err(format!(
                "expected Logon response, got MsgType {:?}",
                std::str::from_utf8(response.msg_type())
            )
            .into());
        }

        Ok(client)
    }

    /// Send a FIX message built from a `FixMessageBuilder`.
    pub fn send_builder(&mut self, builder: FixMessageBuilder) -> io::Result<()> {
        let msg = builder.build(
            &self.sender_comp_id,
            &self.target_comp_id,
            self.outbound_seq,
        );
        self.stream.write_all(&msg)?;
        self.stream.flush()?;
        self.outbound_seq += 1;
        Ok(())
    }

    /// Read one complete FIX message from the connection.
    ///
    /// Blocks until a complete message is available or the read times out.
    /// The returned `FixMessage` borrows from an internal buffer owned by
    /// the client; it is invalidated by the next call to `recv`/`try_recv`.
    pub fn recv(&mut self) -> Result<FixMessage<'_>, Box<dyn std::error::Error>> {
        let mut tmp = [0u8; 4096];
        loop {
            if let Some(raw) = parse::try_extract_message(&mut self.parse_buf) {
                self.recv_buf = raw;
                let msg = FixMessage::parse(&self.recv_buf)?;
                return Ok(msg);
            }

            let n = self.stream.read(&mut tmp)?;
            if n == 0 {
                return Err("connection closed".into());
            }
            self.parse_buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Set the read timeout for subsequent `recv()` calls.
    /// Call with a short duration before `try_recv` loops, or a longer
    /// duration before blocking `recv` calls.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    /// Try to read a FIX message without blocking.
    ///
    /// Requires a short read timeout to have been set via `set_read_timeout`.
    /// Returns `Ok(None)` if no complete message is available yet. The
    /// returned `FixMessage` borrows from an internal buffer owned by the
    /// client; it is invalidated by the next call to `recv`/`try_recv`.
    pub fn try_recv(&mut self) -> Result<Option<FixMessage<'_>>, Box<dyn std::error::Error>> {
        let mut tmp = [0u8; 4096];
        match self.stream.read(&mut tmp) {
            Ok(0) => return Err("connection closed".into()),
            Ok(n) => self.parse_buf.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            }
            Err(e) => return Err(e.into()),
        }

        if let Some(raw) = parse::try_extract_message(&mut self.parse_buf) {
            self.recv_buf = raw;
            let msg = FixMessage::parse(&self.recv_buf)?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    /// Send a Logout and close the connection.
    pub fn logout(&mut self) -> io::Result<()> {
        let logout =
            FixMessageBuilder::new(tags::MSG_LOGOUT).str_tag(tags::TEXT, "client shutdown");
        self.send_builder(logout)?;
        // Best-effort read the Logout response.
        let _ = self.stream.set_read_timeout(Some(Duration::from_secs(1)));
        let _ = self.recv();
        Ok(())
    }

    pub fn sender_comp_id(&self) -> &str {
        &self.sender_comp_id
    }

    pub fn target_comp_id(&self) -> &str {
        &self.target_comp_id
    }

    pub fn next_outbound_seq(&self) -> u64 {
        self.outbound_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fix_client_type_is_constructible() {
        // Smoke test — verify the type compiles and the builder chain works.
        let builder = FixMessageBuilder::new(tags::MSG_LOGON)
            .str_tag(tags::ENCRYPT_METHOD, "0")
            .u64_tag(tags::HEART_BT_INT, 30);
        let msg = builder.build("SENDER", "TARGET", 1);
        let parsed = FixMessage::parse(&msg).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_LOGON);
    }
}
