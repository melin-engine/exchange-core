//! smoltcp `Device` implementation backed by DPDK `rx_burst`/`tx_burst`.
//!
//! This is the bridge between the userspace TCP stack (smoltcp) and the
//! NIC driver (DPDK). smoltcp calls `receive()` to get inbound Ethernet
//! frames and `transmit()` to send outbound frames. We translate these
//! into DPDK mbuf operations.
//!
//! The device is single-threaded — it's called from the DPDK poll thread
//! only. No synchronization needed.

use smoltcp::phy::{self, Device, DeviceCapabilities, Medium};
use smoltcp::time::Instant;

use crate::ffi;

/// Maximum burst size for `rte_eth_rx_burst` / `rte_eth_tx_burst`.
/// 32 is the typical sweet spot: amortizes per-call overhead without
/// adding excessive latency from batch processing.
const BURST_SIZE: usize = 32;

/// MTU for standard Ethernet (no jumbo frames). 1500 bytes payload +
/// 14-byte Ethernet header + 4-byte FCS = 1518. smoltcp handles the
/// Ethernet header, so we advertise the raw Ethernet frame capacity.
const MTU: usize = 1500;

/// smoltcp device backed by a DPDK port. Holds a reference to the port
/// and mempool for mbuf allocation.
pub struct DpdkDevice {
    port_id: u16,
    mempool: *mut ffi::rte_mempool,
    /// Staging buffer for received mbufs. `rx_burst` fills this; we
    /// hand them out one at a time via `receive()`.
    rx_buf: [*mut ffi::rte_mbuf; BURST_SIZE],
    /// Number of valid mbufs in `rx_buf`.
    rx_count: usize,
    /// Next mbuf to hand out from `rx_buf`.
    rx_cursor: usize,
}

// SAFETY: DpdkDevice is only used from the single DPDK poll thread.
// The raw pointers are DPDK mbufs allocated from hugepage memory.
unsafe impl Send for DpdkDevice {}

impl DpdkDevice {
    /// Create a new device for the given DPDK port.
    ///
    /// `mempool` must outlive the device (it's used for TX mbuf allocation).
    pub fn new(port_id: u16, mempool: *mut ffi::rte_mempool) -> Self {
        DpdkDevice {
            port_id,
            mempool,
            rx_buf: [std::ptr::null_mut(); BURST_SIZE],
            rx_count: 0,
            rx_cursor: 0,
        }
    }

    /// Poll the NIC for received packets. Call this at the top of
    /// each poll loop iteration before `iface.poll()`.
    ///
    /// Fills the internal RX buffer with up to BURST_SIZE mbufs.
    pub fn poll_rx(&mut self) {
        if self.rx_cursor < self.rx_count {
            // Still have unprocessed packets from the last burst.
            return;
        }

        // SAFETY: port is started, rx_buf is correctly sized.
        let count = unsafe {
            ffi::rte_eth_rx_burst(
                self.port_id,
                0, // queue_id
                self.rx_buf.as_mut_ptr(),
                BURST_SIZE as u16,
            )
        };

        self.rx_count = count as usize;
        self.rx_cursor = 0;
    }
}

impl Device for DpdkDevice {
    type RxToken<'a> = DpdkRxToken;
    type TxToken<'a> = DpdkTxToken;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.rx_cursor >= self.rx_count {
            return None;
        }

        let mbuf = self.rx_buf[self.rx_cursor];
        self.rx_cursor += 1;

        // Read packet data from the mbuf.
        let (data_ptr, data_len) = unsafe {
            let ptr = (*mbuf).buf_addr.byte_add((*mbuf).data_off as usize);
            let len = (*mbuf).data_len as usize;
            (ptr, len)
        };

        // Copy packet data to a stack buffer. This copy is unavoidable:
        // smoltcp's RxToken::consume takes ownership via a closure, but
        // the mbuf must be freed back to the pool after consumption.
        // At ~1500 bytes max, this is a single memcpy well within L1.
        let mut buf = vec![0u8; data_len];
        unsafe {
            std::ptr::copy_nonoverlapping(data_ptr as *const u8, buf.as_mut_ptr(), data_len);
            ffi::rte_pktmbuf_free(mbuf);
        }

        let rx_token = DpdkRxToken { buf };
        let tx_token = DpdkTxToken {
            port_id: self.port_id,
            mempool: self.mempool,
        };

        Some((rx_token, tx_token))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(DpdkTxToken {
            port_id: self.port_id,
            mempool: self.mempool,
        })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ethernet;
        caps.max_transmission_unit = MTU;
        // No checksum offload for now — smoltcp computes checksums in
        // software. Can enable NIC offload later for marginal gains.
        caps.max_burst_size = Some(BURST_SIZE);
        caps
    }
}

/// RX token: holds one received Ethernet frame.
pub struct DpdkRxToken {
    buf: Vec<u8>,
}

impl phy::RxToken for DpdkRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.buf)
    }
}

/// TX token: allocates an mbuf and sends one Ethernet frame.
pub struct DpdkTxToken {
    port_id: u16,
    mempool: *mut ffi::rte_mempool,
}

impl phy::TxToken for DpdkTxToken {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        // Allocate an mbuf from the pool.
        let mbuf = unsafe { ffi::rte_pktmbuf_alloc(self.mempool) };
        assert!(!mbuf.is_null(), "mbuf alloc failed — mempool exhausted");

        // Get a mutable slice to the mbuf data area and let the caller
        // (smoltcp) write the Ethernet frame into it.
        let data_ptr = unsafe { (*mbuf).buf_addr.byte_add((*mbuf).data_off as usize) as *mut u8 };
        let buf = unsafe { std::slice::from_raw_parts_mut(data_ptr, len) };

        let result = f(buf);

        // Set the packet length fields.
        unsafe {
            (*mbuf).data_len = len as u16;
            (*mbuf).pkt_len = len as u32;
        }

        // Send the packet. tx_burst returns the number of packets sent;
        // if 0, the TX queue is full and we must free the mbuf.
        let mut tx_mbuf = mbuf;
        let sent = unsafe { ffi::rte_eth_tx_burst(self.port_id, 0, &mut tx_mbuf, 1) };
        if sent == 0 {
            unsafe {
                ffi::rte_pktmbuf_free(mbuf);
            }
            tracing::debug!("TX queue full, dropped packet");
        }

        result
    }
}
