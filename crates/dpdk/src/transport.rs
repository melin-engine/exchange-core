//! High-level DPDK transport: combines EAL, port, mempool, and smoltcp
//! into a single poll-driven interface for the trading server.
//!
//! The transport owns the DPDK port and smoltcp interface. The server's
//! DPDK poll thread calls `poll()` in a tight loop to:
//!   1. Receive packets from the NIC via `rte_eth_rx_burst`
//!   2. Process TCP via smoltcp (`interface.poll()`)
//!   3. Accept new TCP connections
//!   4. Read frames from established connections
//!   5. Write pending responses to connections
//!   6. Transmit outbound packets via `rte_eth_tx_burst`

use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{self, State};
use smoltcp::time::Instant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address};

use crate::device::DpdkDevice;
use crate::eal::Eal;
use crate::mempool::Mempool;
use crate::port::Port;

/// Maximum concurrent TCP connections. Limits smoltcp socket set size.
const MAX_CONNECTIONS: usize = 1024;

/// TCP listen port for trading connections.
const LISTEN_PORT: u16 = 9876;

/// TCP receive buffer size per connection. 64 KiB is generous for
/// small trading messages (~100-200 bytes) but provides headroom for
/// FIX messages and pipelining bursts.
const TCP_RX_BUF_SIZE: usize = 65536;

/// TCP send buffer size per connection.
const TCP_TX_BUF_SIZE: usize = 65536;

/// Configuration for the DPDK transport.
pub struct DpdkConfig {
    /// EAL arguments (e.g., `["-l", "0-7", "--huge-dir", "/dev/hugepages"]`).
    pub eal_args: Vec<String>,
    /// DPDK port ID (default 0).
    pub port_id: u16,
    /// IPv4 address for the DPDK interface.
    pub ip_addr: Ipv4Addr,
    /// IPv4 prefix length (e.g., 24 for /24).
    pub prefix_len: u8,
    /// IPv4 gateway (optional, needed for cross-subnet traffic).
    pub gateway: Option<Ipv4Addr>,
    /// TCP listen port.
    pub listen_port: u16,
}

impl Default for DpdkConfig {
    fn default() -> Self {
        DpdkConfig {
            eal_args: Vec::new(),
            port_id: 0,
            ip_addr: Ipv4Addr::new(10, 0, 0, 1),
            prefix_len: 24,
            gateway: None,
            listen_port: LISTEN_PORT,
        }
    }
}

/// A new TCP connection accepted by the transport.
pub struct AcceptedConnection {
    /// smoltcp socket handle for this connection.
    pub handle: SocketHandle,
    /// Peer address.
    pub peer: std::net::SocketAddr,
}

/// The DPDK transport. Owns all DPDK and smoltcp state.
///
/// All methods must be called from the DPDK poll thread — the transport
/// is NOT thread-safe (smoltcp is single-threaded by design).
pub struct DpdkTransport {
    /// DPDK EAL — must outlive port and mempool (drop order matters).
    _eal: Eal,
    /// Packet memory pool.
    _mempool: Mempool,
    /// NIC port.
    _port: Port,
    /// smoltcp device backed by DPDK.
    device: DpdkDevice,
    /// smoltcp network interface (IP layer).
    iface: Interface,
    /// smoltcp socket set (all TCP connections).
    sockets: SocketSet<'static>,
    /// Listening socket handle.
    listen_handle: SocketHandle,
    /// Newly accepted connections (drained by the server each poll iteration).
    accepted: Vec<AcceptedConnection>,
    /// Per-connection TX buffers. The response stage writes encoded frames
    /// here; the poll loop drains them into smoltcp sockets.
    /// Key: socket handle raw index.
    tx_queues: BTreeMap<usize, Vec<u8>>,
}

impl DpdkTransport {
    /// Initialize the DPDK transport: EAL, mempool, port, smoltcp.
    pub fn init(config: &DpdkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // 1. Initialize DPDK EAL.
        let eal_args: Vec<&str> = config.eal_args.iter().map(|s| s.as_str()).collect();
        let eal = Eal::init(&eal_args)?;

        let port_count = eal.port_count();
        if config.port_id >= port_count {
            return Err(format!(
                "DPDK port {} not found (available: {})",
                config.port_id, port_count
            )
            .into());
        }

        // 2. Create packet mempool.
        let mempool = Mempool::create("pktmbuf_pool", 0)?;

        // 3. Configure and start the NIC port.
        let mut port = Port::configure(config.port_id, &mempool)?;
        port.start()?;

        // 4. Create smoltcp device backed by DPDK.
        let mac = port.mac_addr();
        let device = DpdkDevice::new(config.port_id, mempool.as_raw());

        // 5. Create smoltcp network interface.
        let hw_addr = HardwareAddress::Ethernet(EthernetAddress(mac));
        let iface_config = Config::new(hw_addr);
        let mut iface = Interface::new(iface_config, &mut DpdkDeviceRef(&device));

        // Set IP address.
        let ip = Ipv4Address::from(config.ip_addr);
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::Ipv4(ip), config.prefix_len))
                .expect("IP address capacity");
        });

        // Set default gateway if provided.
        if let Some(gw) = config.gateway {
            iface
                .routes_mut()
                .add_default_ipv4_route(Ipv4Address::from(gw))
                .expect("default route capacity");
        }

        // 6. Create socket set and add a listening socket.
        let mut sockets = SocketSet::new(Vec::with_capacity(MAX_CONNECTIONS));

        let listen_socket = {
            // Static buffers for the listening socket (it doesn't transfer
            // data, only does the TCP handshake).
            let rx_buf = tcp::SocketBuffer::new(vec![0u8; 1024]);
            let tx_buf = tcp::SocketBuffer::new(vec![0u8; 1024]);
            let mut socket = tcp::Socket::new(rx_buf, tx_buf);
            socket
                .listen(config.listen_port)
                .map_err(|e| format!("TCP listen failed: {e}"))?;
            socket
        };
        let listen_handle = sockets.add(listen_socket);

        tracing::info!(
            ip = %config.ip_addr,
            port = config.listen_port,
            mac = ?mac,
            "DPDK transport initialized"
        );

        Ok(DpdkTransport {
            _eal: eal,
            _mempool: mempool,
            _port: port,
            device,
            iface,
            sockets,
            listen_handle,
            accepted: Vec::new(),
            tx_queues: BTreeMap::new(),
        })
    }

    /// Run one poll iteration. Call this in a tight loop from the DPDK thread.
    ///
    /// Returns the smoltcp timestamp used for this iteration (for
    /// timer management).
    pub fn poll(&mut self) -> Instant {
        let timestamp = Instant::from_millis(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        );

        // 1. Poll NIC for received packets.
        self.device.poll_rx();

        // 2. Run smoltcp: process inbound packets, advance TCP state machines,
        //    generate outbound packets.
        self.iface
            .poll(timestamp, &mut self.device, &mut self.sockets);

        // 3. Check if the listening socket has accepted a connection.
        self.check_listener();

        // 4. Drain TX queues into smoltcp sockets.
        self.flush_tx_queues();

        timestamp
    }

    /// Check if the listening socket has completed a TCP handshake.
    /// If so, move it to the accepted list and create a new listener.
    fn check_listener(&mut self) {
        let socket = self.sockets.get_mut::<tcp::Socket>(self.listen_handle);
        if socket.state() == State::Established {
            // Extract peer address before we move the socket.
            let peer = if let Some(remote) = socket.remote_endpoint() {
                match remote.addr {
                    IpAddress::Ipv4(ip) => std::net::SocketAddr::new(
                        std::net::IpAddr::V4(Ipv4Addr::from(ip.0)),
                        remote.port,
                    ),
                    _ => return, // IPv6 not supported
                }
            } else {
                return;
            };

            // The accepted connection now owns this socket handle. Record it.
            let accepted_handle = self.listen_handle;

            // Create a new listening socket to replace the one that was consumed.
            let new_listener = {
                let rx_buf = tcp::SocketBuffer::new(vec![0u8; 1024]);
                let tx_buf = tcp::SocketBuffer::new(vec![0u8; 1024]);
                let mut socket = tcp::Socket::new(rx_buf, tx_buf);
                socket.listen(LISTEN_PORT).expect("re-listen after accept");
                socket
            };
            self.listen_handle = self.sockets.add(new_listener);

            self.accepted.push(AcceptedConnection {
                handle: accepted_handle,
                peer,
            });

            tracing::debug!(peer = %peer, "DPDK: TCP connection accepted");
        }
    }

    /// Drain pending TX data from per-connection queues into smoltcp sockets.
    fn flush_tx_queues(&mut self) {
        // Collect handles to avoid borrow conflict.
        let handles: Vec<usize> = self.tx_queues.keys().copied().collect();

        for handle_idx in handles {
            let queue = match self.tx_queues.get_mut(&handle_idx) {
                Some(q) if !q.is_empty() => q,
                _ => continue,
            };

            // Reconstruct the SocketHandle from the raw index.
            // SAFETY: we stored this index when the connection was accepted.
            let handle = unsafe { SocketHandle::from_usize(handle_idx) };
            let socket = self.sockets.get_mut::<tcp::Socket>(handle);

            if !socket.can_send() {
                continue;
            }

            let sent = socket.send_slice(queue).unwrap_or(0);
            if sent > 0 {
                queue.drain(..sent);
            }
        }
    }

    /// Take all newly accepted connections. Called by the server after `poll()`.
    pub fn take_accepted(&mut self) -> Vec<AcceptedConnection> {
        std::mem::take(&mut self.accepted)
    }

    /// Read available data from a connection into `buf`.
    /// Returns the number of bytes read, or 0 if no data available.
    pub fn recv(&mut self, handle: SocketHandle, buf: &mut [u8]) -> usize {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        if !socket.can_recv() {
            return 0;
        }
        socket.recv_slice(buf).unwrap_or(0)
    }

    /// Queue data to be sent on a connection. The data is buffered and
    /// flushed to smoltcp during the next `poll()` call.
    ///
    /// This method is safe to call from the response stage thread via
    /// a lock-free queue (see `dpdk_transport.rs` in the server crate).
    pub fn queue_send(&mut self, handle: SocketHandle, data: &[u8]) {
        let handle_idx = handle.into();
        self.tx_queues
            .entry(handle_idx)
            .or_insert_with(Vec::new)
            .extend_from_slice(data);
    }

    /// Check if a connection is still open.
    pub fn is_active(&mut self, handle: SocketHandle) -> bool {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        socket.is_active()
    }

    /// Close a connection (sends FIN).
    pub fn close(&mut self, handle: SocketHandle) {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        socket.close();
        let handle_idx: usize = handle.into();
        self.tx_queues.remove(&handle_idx);
    }
}

/// Temporary wrapper to pass `&DpdkDevice` where smoltcp wants `&mut impl Device`.
/// Used only during `Interface::new` which needs a device reference for
/// capability probing. The actual mutable device is passed during `poll()`.
struct DpdkDeviceRef<'a>(&'a DpdkDevice);

impl<'a> smoltcp::phy::Device for DpdkDeviceRef<'a> {
    type RxToken<'b>
        = crate::device::DpdkRxToken
    where
        Self: 'b;
    type TxToken<'b>
        = crate::device::DpdkTxToken
    where
        Self: 'b;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        None // Never called during Interface::new
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        None // Never called during Interface::new
    }

    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.0.capabilities()
    }
}
