//! smoltcp `Device` implementation backed by DPDK `rx_burst`/`tx_burst`.
//!
//! This is the bridge between the userspace TCP stack (smoltcp) and the
//! NIC driver (DPDK). smoltcp calls `receive()` to get inbound Ethernet
//! frames and `transmit()` to send outbound frames. We translate these
//! into DPDK mbuf operations via C wrapper functions (see inline_wrappers.c).
//!
//! The device is single-threaded — it's called from the DPDK poll thread
//! only. No synchronization needed.

use smoltcp::phy::{self, Checksum, ChecksumCapabilities, Device, DeviceCapabilities, Medium};
use smoltcp::time::Instant;

use crate::ffi;
use crate::port::ChecksumOffloads;

/// Maximum burst size for rx_burst / tx_burst.
/// 32 is the typical sweet spot: amortizes per-call overhead without
/// adding excessive latency from batch processing.
const BURST_SIZE: usize = 32;

/// MTU for standard Ethernet (no jumbo frames).
const MTU: usize = 1500;

/// smoltcp device backed by a DPDK port.
pub struct DpdkDevice {
    port_id: u16,
    mempool: *mut ffi::rte_mempool,
    /// Staging buffer for received mbufs.
    rx_buf: [*mut ffi::rte_mbuf; BURST_SIZE],
    rx_count: usize,
    rx_cursor: usize,
    /// Hardware checksum offloads supported by the NIC.
    offloads: ChecksumOffloads,
    /// Cached TX offload flags (computed once at init, reused per packet).
    tx_ol_flags: u64,
}

// SAFETY: DpdkDevice is only used from the single DPDK poll thread.
unsafe impl Send for DpdkDevice {}

impl DpdkDevice {
    /// Create a new device for the given DPDK port.
    pub fn new(port_id: u16, mempool: *mut ffi::rte_mempool, offloads: ChecksumOffloads) -> Self {
        // Pre-compute TX offload flags once — these are the same for every
        // outbound IPv4/TCP packet.
        let mut tx_ol_flags: u64 = 0;
        if offloads.tx_ip {
            tx_ol_flags |= unsafe { ffi::dpdk_tx_offload_ipv4_cksum() };
        }
        if offloads.tx_tcp {
            tx_ol_flags |= unsafe { ffi::dpdk_tx_offload_tcp_cksum() };
        }
        if tx_ol_flags != 0 {
            tracing::info!("DPDK TX checksum offload enabled (flags=0x{tx_ol_flags:x})");
        }

        DpdkDevice {
            port_id,
            mempool,
            rx_buf: [std::ptr::null_mut(); BURST_SIZE],
            rx_count: 0,
            rx_cursor: 0,
            offloads,
            tx_ol_flags,
        }
    }

    /// Poll the NIC for received packets.
    pub fn poll_rx(&mut self) {
        if self.rx_cursor < self.rx_count {
            return;
        }

        // SAFETY: port is started, rx_buf is correctly sized.
        let count = unsafe {
            ffi::dpdk_eth_rx_burst(self.port_id, 0, self.rx_buf.as_mut_ptr(), BURST_SIZE as u16)
        };

        self.rx_count = count as usize;
        self.rx_cursor = 0;
    }

    /// Capabilities accessor for use by DpdkDeviceRef.
    pub fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ethernet;
        caps.max_transmission_unit = MTU;
        caps.max_burst_size = Some(BURST_SIZE);

        // Tell smoltcp which checksums the NIC handles in hardware.
        // `Checksum::None` means "don't compute or verify" — the NIC does it.
        let mut checksums = ChecksumCapabilities::default();
        if self.offloads.rx_ip && self.offloads.tx_ip {
            checksums.ipv4 = Checksum::None;
        } else if self.offloads.tx_ip {
            checksums.ipv4 = Checksum::Rx; // verify on RX only
        } else if self.offloads.rx_ip {
            checksums.ipv4 = Checksum::Tx; // compute on TX only
        }
        if self.offloads.rx_tcp && self.offloads.tx_tcp {
            checksums.tcp = Checksum::None;
        } else if self.offloads.tx_tcp {
            checksums.tcp = Checksum::Rx;
        } else if self.offloads.rx_tcp {
            checksums.tcp = Checksum::Tx;
        }
        caps.checksum = checksums;

        caps
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

        // Read packet data via C accessors (avoids direct struct field access
        // on bindgen-generated types with complex unions/bitfields).
        let (data_ptr, data_len) = unsafe {
            let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf).cast::<u8>();
            let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
            let ptr = buf_addr.add(data_off);
            let len = ffi::dpdk_mbuf_data_len(mbuf) as usize;
            (ptr, len)
        };

        // Pass the mbuf directly to the RxToken. The token holds the raw
        // pointer and frees it after smoltcp consumes the packet data.
        // This avoids any copy or allocation — smoltcp reads directly
        // from DPDK hugepage memory.
        let rx_token = DpdkRxToken {
            mbuf,
            data_ptr: data_ptr as *const u8,
            data_len,
        };
        let tx_token = DpdkTxToken {
            port_id: self.port_id,
            mempool: self.mempool,
            tx_ol_flags: self.tx_ol_flags,
        };

        Some((rx_token, tx_token))
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        Some(DpdkTxToken {
            port_id: self.port_id,
            mempool: self.mempool,
            tx_ol_flags: self.tx_ol_flags,
        })
    }

    fn capabilities(&self) -> DeviceCapabilities {
        self.capabilities()
    }
}

/// RX token: holds one received Ethernet frame via a DPDK mbuf.
/// Zero-copy: smoltcp reads directly from hugepage-backed mbuf memory.
/// The mbuf is freed back to the pool when the token is consumed.
pub struct DpdkRxToken {
    mbuf: *mut ffi::rte_mbuf,
    data_ptr: *const u8,
    data_len: usize,
}

impl phy::RxToken for DpdkRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        // SAFETY: data_ptr points into the mbuf's data area which remains
        // valid until rte_pktmbuf_free is called. We call f() first, then free.
        let data = unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_len) };
        let result = f(data);
        unsafe {
            ffi::dpdk_pktmbuf_free(self.mbuf);
        }
        result
    }
}

/// TX token: allocates an mbuf and sends one Ethernet frame.
pub struct DpdkTxToken {
    port_id: u16,
    mempool: *mut ffi::rte_mempool,
    /// Pre-computed TX offload flags (IPv4 + TCP checksum offload).
    tx_ol_flags: u64,
}

impl phy::TxToken for DpdkTxToken {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mbuf = unsafe { ffi::dpdk_pktmbuf_alloc(self.mempool) };
        assert!(!mbuf.is_null(), "mbuf alloc failed — mempool exhausted");

        // Get mutable slice via C accessors. Cast from *mut c_void to
        // *mut u8 (dpdk_mbuf_buf_addr returns void*).
        let data_ptr = unsafe {
            let buf_addr = ffi::dpdk_mbuf_buf_addr(mbuf).cast::<u8>();
            let data_off = ffi::dpdk_mbuf_data_off(mbuf) as usize;
            buf_addr.add(data_off)
        };
        let buf = unsafe { std::slice::from_raw_parts_mut(data_ptr, len) };

        let result = f(buf);

        // Set packet length via C accessors.
        unsafe {
            ffi::dpdk_mbuf_set_data_len(mbuf, len as u16);
            ffi::dpdk_mbuf_set_pkt_len(mbuf, len as u32);

            // Set hardware checksum offload flags if the NIC supports it.
            // The NIC needs ol_flags to know what to offload, and l2_len/l3_len
            // to locate the IP and TCP headers within the frame.
            if self.tx_ol_flags != 0 && len > 14 + 20 {
                // Ethernet header = 14 bytes, IPv4 header = 20 bytes (no options).
                // smoltcp doesn't use IP options or VLAN tags at L2.
                ffi::dpdk_mbuf_set_ol_flags(mbuf, self.tx_ol_flags);
                ffi::dpdk_mbuf_set_tx_offload(mbuf, 14, 20, 0);
            }
        }

        let mut tx_mbuf = mbuf;
        let sent = unsafe { ffi::dpdk_eth_tx_burst(self.port_id, 0, &mut tx_mbuf, 1) };
        if sent == 0 {
            unsafe {
                ffi::dpdk_pktmbuf_free(mbuf);
            }
            tracing::debug!("TX queue full, dropped packet");
        }

        result
    }
}
