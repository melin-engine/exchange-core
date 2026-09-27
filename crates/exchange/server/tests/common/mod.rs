//! Helpers shared by the integration tests that run `melin-ec-server` as a
//! child process.

use std::path::PathBuf;

/// Path to the `melin-ec-server` binary, resolved at compile time. `env!` works
/// under any runner; `std::env::var` only works under `cargo test`, where it
/// would otherwise silently pick up a stale release binary.
pub fn server_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_melin-ec-server"))
}

/// Allocate a unique TCP port for a test-spawned server.
///
/// Previous implementation bound an ephemeral port (`127.0.0.1:0`) and
/// returned it after dropping the listener. That left a race window: the
/// kernel could hand the same port to a concurrent test before the
/// spawned child actually bound it, producing flakes that look like
/// `Address already in use`, `Connection reset`, or — most insidiously
/// — wrong-protocol bytes leaking between tests (e.g. text from the
/// health endpoint hitting a replication socket and surfacing as
/// `frame too large: <ascii-as-int>`).
///
/// nextest spawns each test in its own process, so a per-process atomic
/// can't coordinate across tests either. Use a file-locked counter in
/// `/tmp` — every test process across the binary's run shares the same
/// counter, advancing it monotonically. Ports live in 20000..32000,
/// below the kernel's ephemeral range (default 32768..60999) so the
/// kernel never hands out a port we've already reserved.
pub fn free_port() -> u16 {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::fd::AsRawFd;

    const PORT_FILE: &str = "/tmp/melin_test_port_alloc";
    const PORT_FLOOR: u16 = 20_000;
    const PORT_CEILING: u16 = 32_000;

    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(PORT_FILE)
        .expect("open port allocator file");

    // SAFETY: flock(2) on a valid fd; LOCK_EX blocks until acquired.
    let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) };
    assert!(rc == 0, "flock failed: {}", std::io::Error::last_os_error());

    let mut s = String::new();
    let _ = f.read_to_string(&mut s);
    let next: u16 = s.trim().parse().unwrap_or(PORT_FLOOR);
    let port = if next >= PORT_CEILING {
        PORT_FLOOR
    } else {
        next
    };
    let after = port + 1;

    f.seek(SeekFrom::Start(0)).expect("seek port file");
    f.set_len(0).expect("truncate port file");
    write!(f, "{after}").expect("write port file");

    // SAFETY: same fd we LOCK_EX'd above.
    let _ = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_UN) };

    port
}
