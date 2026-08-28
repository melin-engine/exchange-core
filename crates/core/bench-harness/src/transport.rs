//! Client-side connection helpers.
//!
//! Both retry, because a benchmark usually starts its load generator
//! immediately after starting the server it is measuring, and losing the
//! race is normal rather than an error.

use std::os::unix::io::AsRawFd;
use std::time::Duration;

/// Connection attempts before giving up, and the pause between them —
/// half a second of total patience, enough to cover a server still
/// binding its listener.
const CONNECT_ATTEMPTS: usize = 50;
const CONNECT_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Connect to a TCP server with retry.
///
/// Also enables `SO_BUSY_POLL` on the connected socket. The bench's
/// io_uring loop already busy-spins on CQEs, so the kernel's NIC
/// busy-poll uses cycles that would otherwise be wasted spinning, and
/// it removes the softirq → wakeup handoff from every server response
/// — tightening the bench's measurement floor so we observe the
/// server's true latency rather than the bench's own client-side
/// scheduler jitter.
pub fn connect_tcp(addr: std::net::SocketAddr) -> std::net::TcpStream {
    let mut last_err = None;
    for _ in 0..CONNECT_ATTEMPTS {
        match std::net::TcpStream::connect(addr) {
            Ok(s) => {
                // Best-effort SO_BUSY_POLL; failure is logged via stderr
                // but does not abort the bench (the kernel may reject
                // it without CAP_NET_ADMIN, in which case we measure
                // with the default scheduler-wakeup cost — still
                // accurate, just slightly noisier).
                let val: libc::c_int = 50;
                let rc = unsafe {
                    libc::setsockopt(
                        s.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_BUSY_POLL,
                        &val as *const libc::c_int as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    )
                };
                if rc != 0 {
                    let err = std::io::Error::last_os_error();
                    eprintln!("warning: SO_BUSY_POLL setsockopt failed: {err}");
                }
                return s;
            }
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(CONNECT_RETRY_DELAY);
            }
        }
    }
    panic!(
        "failed to connect after {CONNECT_ATTEMPTS} attempts: {}",
        last_err.expect("at least one attempt failed")
    );
}

/// Connect to a Unix-domain-socket server with retry.
pub fn connect_uds(path: &std::path::Path) -> std::os::unix::net::UnixStream {
    let mut last_err = None;
    for _ in 0..CONNECT_ATTEMPTS {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(s) => return s,
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(CONNECT_RETRY_DELAY);
            }
        }
    }
    panic!(
        "failed to connect after {CONNECT_ATTEMPTS} attempts: {}",
        last_err.expect("at least one attempt failed")
    );
}
