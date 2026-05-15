//! ITCH 5.0 binary decoder.
//!
//! Streams a daily ITCH file (typically gzip-compressed) and yields
//! one [`ItchEvent`] per recognized message. Unknown message types
//! are skipped using the 2-byte big-endian frame length prefix that
//! wraps every ITCH message, so the parser is robust to future ITCH
//! revisions adding message types.
//!
//! Only continuous-session message types relevant to engine-load
//! calibration are decoded: Add (A/F), Execute (E/C), Cancel (X),
//! Delete (D), Replace (U), Hidden Trade (P), and Stock Directory (R,
//! to build a stock-locate→ticker map for symbol filtering). Cross
//! trades (Q) are intentionally skipped — auction events have a
//! different microstructure and would distort marginals.

use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

use flate2::read::GzDecoder;

use super::Side;

/// One decoded ITCH 5.0 message. Faithful to the wire layout; symbol
/// filtering happens downstream via the [`ItchEvent::stock_locate`]
/// codes resolved against the start-of-day [`ItchEvent::StockDirectory`]
/// messages.
#[derive(Debug, Clone)]
pub enum ItchEvent {
    /// Stock Directory ('R'): emitted at start-of-day, maps a stock
    /// locate code to its 8-char ticker. The extractor builds the
    /// locate→ticker table from these and filters subsequent events
    /// by it.
    StockDirectory { stock_locate: u16, stock: [u8; 8] },
    /// Add Order, no MPID ('A').
    AddOrder {
        stock_locate: u16,
        order_ref: u64,
        side: Side,
        shares: u32,
        stock: [u8; 8],
        price: u32,
    },
    /// Add Order with MPID attribution ('F'). MPID is dropped because
    /// calibration doesn't currently use participant identity.
    AddOrderAttributed {
        stock_locate: u16,
        order_ref: u64,
        side: Side,
        shares: u32,
        stock: [u8; 8],
        price: u32,
    },
    /// Order Executed ('E'). No explicit price — execution is at the
    /// order's resting price, which the stats layer resolves via a
    /// book tracker.
    OrderExecuted {
        stock_locate: u16,
        order_ref: u64,
        shares: u32,
    },
    /// Order Executed With Price ('C'). Execution price differs from
    /// the order's displayed price (typically a price-improvement or
    /// non-displayed liquidity match).
    OrderExecutedWithPrice {
        stock_locate: u16,
        order_ref: u64,
        shares: u32,
        exec_price: u32,
    },
    /// Order Cancel ('X'): partial cancel reducing displayed quantity.
    OrderCancel {
        stock_locate: u16,
        order_ref: u64,
        cancelled_shares: u32,
    },
    /// Order Delete ('D'): full cancel.
    OrderDelete { stock_locate: u16, order_ref: u64 },
    /// Order Replace ('U'): atomic cancel-and-add at a new order ref.
    OrderReplace {
        stock_locate: u16,
        old_order_ref: u64,
        new_order_ref: u64,
        shares: u32,
        price: u32,
    },
    /// Trade Non-Cross ('P'): execution of a hidden (non-displayed)
    /// order. There is no Add/Delete pair surrounding this; the order
    /// existed but was never on the visible book.
    HiddenTrade {
        stock_locate: u16,
        side: Side,
        shares: u32,
        stock: [u8; 8],
        price: u32,
    },
}

/// Errors surfaced by the parser. EOF at a frame boundary is *not* an
/// error — it returns `Ok(None)` from [`ItchParser::next_event`].
#[derive(Debug)]
pub enum ItchParseError {
    /// Stream ended mid-frame: the 2-byte length prefix said N bytes
    /// were coming but the body was truncated. Indicates corruption
    /// or a partial download.
    UnexpectedEof,
    /// A recognized message type carried a body length that didn't
    /// match the spec. Suggests either a spec drift or that the file
    /// isn't actually ITCH 5.0. Carries the type byte and the
    /// (length, expected) so callers can log usefully.
    UnexpectedLength {
        msg_type: u8,
        actual: usize,
        expected: usize,
    },
    /// A side byte was something other than `'B'` or `'S'`.
    InvalidSide(u8),
    /// Underlying I/O failure.
    Io(io::Error),
}

impl From<io::Error> for ItchParseError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::fmt::Display for ItchParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "ITCH stream ended mid-frame"),
            Self::UnexpectedLength {
                msg_type,
                actual,
                expected,
            } => write!(
                f,
                "ITCH message '{}' had length {actual}, expected {expected}",
                *msg_type as char
            ),
            Self::InvalidSide(b) => write!(f, "ITCH side byte was 0x{b:02x}, expected 'B' or 'S'"),
            Self::Io(e) => write!(f, "ITCH I/O error: {e}"),
        }
    }
}

impl std::error::Error for ItchParseError {}

/// Streaming ITCH 5.0 parser. Holds a reusable body buffer so the hot
/// loop allocates only once.
pub struct ItchParser<R: BufRead> {
    reader: R,
    // Reused per-frame body buffer. Sized to the largest ITCH 5.0
    // message body (50 bytes for NOII), but grown if a future revision
    // exceeds that. Vec<u8> over [u8; N] so we don't have to fail on
    // hypothetical longer messages — calibration doesn't need to be
    // forward-compatible-strict.
    body_buf: Vec<u8>,
}

/// Convenience constructor for a gzipped ITCH dump. Wraps the file in
/// a 1 MiB read buffer before decompression and a 1 MiB buffer after,
/// which empirically gives near-optimal flate2 throughput.
pub fn open_gz_path(path: &Path) -> io::Result<ItchParser<BufReader<GzDecoder<BufReader<File>>>>> {
    // 1 MiB read-ahead buffers — large enough to amortize syscall and
    // miniz_oxide block overhead, small enough to stay in L2.
    const BUF_BYTES: usize = 1 << 20;
    let file = File::open(path)?;
    let raw_buf = BufReader::with_capacity(BUF_BYTES, file);
    let gz = GzDecoder::new(raw_buf);
    let decompressed_buf = BufReader::with_capacity(BUF_BYTES, gz);
    Ok(ItchParser::new(decompressed_buf))
}

impl<R: BufRead> ItchParser<R> {
    pub fn new(reader: R) -> Self {
        // 64 bytes covers every ITCH 5.0 message body with headroom.
        Self {
            reader,
            body_buf: Vec::with_capacity(64),
        }
    }

    /// Read and decode the next frame. Returns `Ok(None)` at EOF.
    /// Unknown message types are skipped silently and the next
    /// recognized one is returned, so callers can treat this as
    /// "give me the next event I care about."
    pub fn next_event(&mut self) -> Result<Option<ItchEvent>, ItchParseError> {
        loop {
            let mut length_buf = [0u8; 2];
            match self.reader.read_exact(&mut length_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e.into()),
            }
            let body_len = u16::from_be_bytes(length_buf) as usize;
            self.body_buf.resize(body_len, 0);
            self.reader
                .read_exact(&mut self.body_buf)
                .map_err(|e| match e.kind() {
                    io::ErrorKind::UnexpectedEof => ItchParseError::UnexpectedEof,
                    _ => ItchParseError::Io(e),
                })?;

            if body_len == 0 {
                continue;
            }
            let msg_type = self.body_buf[0];
            match decode_body(msg_type, &self.body_buf)? {
                Some(event) => return Ok(Some(event)),
                None => continue,
            }
        }
    }
}

fn decode_body(msg_type: u8, body: &[u8]) -> Result<Option<ItchEvent>, ItchParseError> {
    // Body layouts below are 0-indexed from the type byte. Field
    // offsets and widths come from the ITCH 5.0 spec (linked at the
    // top of this file).
    match msg_type {
        b'R' => {
            // Stock Directory: 39-byte body. We only care about the
            // locate→ticker mapping, so most fields are ignored.
            expect_len(msg_type, body.len(), 39)?;
            let stock_locate = read_u16(body, 1);
            let stock = read_stock(body, 11);
            Ok(Some(ItchEvent::StockDirectory {
                stock_locate,
                stock,
            }))
        }
        b'A' => {
            expect_len(msg_type, body.len(), 36)?;
            let stock_locate = read_u16(body, 1);
            let order_ref = read_u64(body, 11);
            let side = decode_side(body[19])?;
            let shares = read_u32(body, 20);
            let stock = read_stock(body, 24);
            let price = read_u32(body, 32);
            Ok(Some(ItchEvent::AddOrder {
                stock_locate,
                order_ref,
                side,
                shares,
                stock,
                price,
            }))
        }
        b'F' => {
            expect_len(msg_type, body.len(), 40)?;
            let stock_locate = read_u16(body, 1);
            let order_ref = read_u64(body, 11);
            let side = decode_side(body[19])?;
            let shares = read_u32(body, 20);
            let stock = read_stock(body, 24);
            let price = read_u32(body, 32);
            Ok(Some(ItchEvent::AddOrderAttributed {
                stock_locate,
                order_ref,
                side,
                shares,
                stock,
                price,
            }))
        }
        b'E' => {
            expect_len(msg_type, body.len(), 31)?;
            let stock_locate = read_u16(body, 1);
            let order_ref = read_u64(body, 11);
            let shares = read_u32(body, 19);
            Ok(Some(ItchEvent::OrderExecuted {
                stock_locate,
                order_ref,
                shares,
            }))
        }
        b'C' => {
            expect_len(msg_type, body.len(), 36)?;
            let stock_locate = read_u16(body, 1);
            let order_ref = read_u64(body, 11);
            let shares = read_u32(body, 19);
            // body[31] is the "printable" flag (Y/N): whether this
            // execution should appear on the public trade tape. We
            // include both for calibration since the engine sees the
            // execution regardless of whether it prints.
            let exec_price = read_u32(body, 32);
            Ok(Some(ItchEvent::OrderExecutedWithPrice {
                stock_locate,
                order_ref,
                shares,
                exec_price,
            }))
        }
        b'X' => {
            expect_len(msg_type, body.len(), 23)?;
            let stock_locate = read_u16(body, 1);
            let order_ref = read_u64(body, 11);
            let cancelled_shares = read_u32(body, 19);
            Ok(Some(ItchEvent::OrderCancel {
                stock_locate,
                order_ref,
                cancelled_shares,
            }))
        }
        b'D' => {
            expect_len(msg_type, body.len(), 19)?;
            let stock_locate = read_u16(body, 1);
            let order_ref = read_u64(body, 11);
            Ok(Some(ItchEvent::OrderDelete {
                stock_locate,
                order_ref,
            }))
        }
        b'U' => {
            expect_len(msg_type, body.len(), 35)?;
            let stock_locate = read_u16(body, 1);
            let old_order_ref = read_u64(body, 11);
            let new_order_ref = read_u64(body, 19);
            let shares = read_u32(body, 27);
            let price = read_u32(body, 31);
            Ok(Some(ItchEvent::OrderReplace {
                stock_locate,
                old_order_ref,
                new_order_ref,
                shares,
                price,
            }))
        }
        b'P' => {
            expect_len(msg_type, body.len(), 44)?;
            let stock_locate = read_u16(body, 1);
            let side = decode_side(body[19])?;
            let shares = read_u32(body, 20);
            let stock = read_stock(body, 24);
            let price = read_u32(body, 32);
            Ok(Some(ItchEvent::HiddenTrade {
                stock_locate,
                side,
                shares,
                stock,
                price,
            }))
        }
        // System Event (S), Trading Action (H), Reg SHO (Y), Market
        // Participant (L), MWCB (V, W), IPO (K), LULD (J), Operational
        // Halt (h), Cross Trade (Q), Broken Trade (B), NOII (I), RPII
        // (N) — irrelevant to engine-load calibration. The 2-byte
        // length prefix already advanced the reader past them.
        _ => Ok(None),
    }
}

#[inline]
fn expect_len(msg_type: u8, actual: usize, expected: usize) -> Result<(), ItchParseError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ItchParseError::UnexpectedLength {
            msg_type,
            actual,
            expected,
        })
    }
}

#[inline]
fn read_u16(body: &[u8], off: usize) -> u16 {
    u16::from_be_bytes([body[off], body[off + 1]])
}

#[inline]
fn read_u32(body: &[u8], off: usize) -> u32 {
    u32::from_be_bytes([body[off], body[off + 1], body[off + 2], body[off + 3]])
}

#[inline]
fn read_u64(body: &[u8], off: usize) -> u64 {
    u64::from_be_bytes([
        body[off],
        body[off + 1],
        body[off + 2],
        body[off + 3],
        body[off + 4],
        body[off + 5],
        body[off + 6],
        body[off + 7],
    ])
}

#[inline]
fn read_stock(body: &[u8], off: usize) -> [u8; 8] {
    let mut out = [0u8; 8];
    out.copy_from_slice(&body[off..off + 8]);
    out
}

#[inline]
fn decode_side(b: u8) -> Result<Side, ItchParseError> {
    match b {
        b'B' => Ok(Side::Buy),
        b'S' => Ok(Side::Sell),
        other => Err(ItchParseError::InvalidSide(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Build a single ITCH frame: 2-byte big-endian length prefix +
    /// body. Helper for hand-crafted parser tests.
    fn frame(body: &[u8]) -> Vec<u8> {
        let len = body.len() as u16;
        let mut v = Vec::with_capacity(2 + body.len());
        v.extend_from_slice(&len.to_be_bytes());
        v.extend_from_slice(body);
        v
    }

    /// Build a synthetic 'A' (Add Order) message body. Per spec:
    /// type(1) stock_locate(2) tracking(2) timestamp(6) order_ref(8)
    /// side(1) shares(4) stock(8) price(4) = 36 bytes.
    fn add_order_body(
        stock_locate: u16,
        order_ref: u64,
        side: u8,
        shares: u32,
        stock: &[u8; 8],
        price: u32,
    ) -> Vec<u8> {
        let mut b = Vec::with_capacity(36);
        b.push(b'A');
        b.extend_from_slice(&stock_locate.to_be_bytes());
        b.extend_from_slice(&0u16.to_be_bytes()); // tracking
        b.extend_from_slice(&[0u8; 6]); // timestamp
        b.extend_from_slice(&order_ref.to_be_bytes());
        b.push(side);
        b.extend_from_slice(&shares.to_be_bytes());
        b.extend_from_slice(stock);
        b.extend_from_slice(&price.to_be_bytes());
        assert_eq!(b.len(), 36);
        b
    }

    #[test]
    fn parses_add_order() {
        let body = add_order_body(42, 0xDEAD_BEEF, b'B', 100, b"TEST1   ", 1_900_000);
        let mut parser = ItchParser::new(Cursor::new(frame(&body)));
        match parser.next_event().unwrap() {
            Some(ItchEvent::AddOrder {
                stock_locate,
                order_ref,
                side,
                shares,
                stock,
                price,
            }) => {
                assert_eq!(stock_locate, 42);
                assert_eq!(order_ref, 0xDEAD_BEEF);
                assert_eq!(side, Side::Buy);
                assert_eq!(shares, 100);
                assert_eq!(&stock, b"TEST1   ");
                assert_eq!(price, 1_900_000);
            }
            other => panic!("expected AddOrder, got {other:?}"),
        }
        assert!(parser.next_event().unwrap().is_none(), "expected EOF");
    }

    #[test]
    fn parses_order_delete() {
        // 'D' Order Delete — 19 bytes body.
        let mut body = Vec::with_capacity(19);
        body.push(b'D');
        body.extend_from_slice(&7u16.to_be_bytes()); // stock_locate
        body.extend_from_slice(&0u16.to_be_bytes()); // tracking
        body.extend_from_slice(&[0u8; 6]); // timestamp
        body.extend_from_slice(&0x1234_5678_u64.to_be_bytes()); // order_ref
        assert_eq!(body.len(), 19);

        let mut parser = ItchParser::new(Cursor::new(frame(&body)));
        match parser.next_event().unwrap() {
            Some(ItchEvent::OrderDelete {
                stock_locate,
                order_ref,
            }) => {
                assert_eq!(stock_locate, 7);
                assert_eq!(order_ref, 0x1234_5678);
            }
            other => panic!("expected OrderDelete, got {other:?}"),
        }
    }

    #[test]
    fn skips_unknown_message_types() {
        // Unknown type 'Z' followed by a real 'D' message. The parser
        // should advance past 'Z' using the frame length and yield
        // the 'D' next.
        let unknown_body = vec![b'Z', 0x00, 0x00, 0x00];
        let mut delete_body = Vec::with_capacity(19);
        delete_body.push(b'D');
        delete_body.extend_from_slice(&1u16.to_be_bytes());
        delete_body.extend_from_slice(&[0u8; 2]);
        delete_body.extend_from_slice(&[0u8; 6]);
        delete_body.extend_from_slice(&99u64.to_be_bytes());

        let mut stream = frame(&unknown_body);
        stream.extend_from_slice(&frame(&delete_body));
        let mut parser = ItchParser::new(Cursor::new(stream));
        match parser.next_event().unwrap() {
            Some(ItchEvent::OrderDelete { order_ref, .. }) => assert_eq!(order_ref, 99),
            other => panic!("expected OrderDelete after skip, got {other:?}"),
        }
    }

    #[test]
    fn rejects_invalid_side() {
        let body = add_order_body(1, 1, b'X', 1, b"TEST    ", 1);
        let mut parser = ItchParser::new(Cursor::new(frame(&body)));
        match parser.next_event() {
            Err(ItchParseError::InvalidSide(b'X')) => {}
            other => panic!("expected InvalidSide error, got {other:?}"),
        }
    }

    #[test]
    fn truncated_body_yields_unexpected_eof() {
        // Length prefix says 10 bytes but only 3 follow.
        let mut stream = 10u16.to_be_bytes().to_vec();
        stream.extend_from_slice(&[b'D', 0x00, 0x00]);
        let mut parser = ItchParser::new(Cursor::new(stream));
        match parser.next_event() {
            Err(ItchParseError::UnexpectedEof) => {}
            other => panic!("expected UnexpectedEof, got {other:?}"),
        }
    }

    #[test]
    fn clean_eof_at_frame_boundary_returns_none() {
        let parser_eof = ItchParser::new(Cursor::new(Vec::<u8>::new()));
        let mut p = parser_eof;
        assert!(p.next_event().unwrap().is_none());
    }
}
