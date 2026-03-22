//! DPDK Environment Abstraction Layer (EAL) initialization.
//!
//! The EAL must be initialized before any other DPDK calls. It sets up
//! hugepage memory, discovers NIC ports, and initializes per-lcore data
//! structures. Initialization happens once at process startup.

use std::ffi::CString;

use crate::ffi;

/// RAII wrapper for EAL initialization. Calls `rte_eal_cleanup` on drop.
pub struct Eal {
    _private: (),
}

impl Eal {
    /// Initialize the DPDK EAL with the given arguments.
    ///
    /// Typical args:
    /// - `["-l", "0-7"]` — logical core mask
    /// - `["--huge-dir", "/dev/hugepages"]` — hugepage mount point
    /// - `["--socket-mem", "1024"]` — memory per NUMA socket in MB
    /// - `["--vdev", "net_tap0"]` — virtual device for testing (no real NIC)
    ///
    /// # Errors
    /// Returns an error if EAL initialization fails (e.g., no hugepages,
    /// insufficient permissions, invalid arguments).
    pub fn init(args: &[&str]) -> Result<Self, EalError> {
        // Convert args to C strings. EAL expects argv[0] to be the program
        // name (it's ignored but must be present).
        let mut c_args: Vec<CString> = Vec::with_capacity(args.len() + 1);
        c_args.push(CString::new("melin-dpdk").expect("program name"));
        for arg in args {
            c_args.push(CString::new(*arg).map_err(|_| EalError::InvalidArg)?);
        }

        let mut c_ptrs: Vec<*mut libc::c_char> = c_args
            .iter()
            .map(|s| s.as_ptr() as *mut libc::c_char)
            .collect();

        let argc = c_ptrs.len() as libc::c_int;

        // SAFETY: rte_eal_init is called once at startup with valid argc/argv.
        // The CStrings remain alive for the duration of the call.
        let ret = unsafe { ffi::rte_eal_init(argc, c_ptrs.as_mut_ptr()) };

        if ret < 0 {
            return Err(EalError::InitFailed(ret));
        }

        tracing::info!(cores = ret, "DPDK EAL initialized");
        Ok(Eal { _private: () })
    }

    /// Number of available DPDK ethernet ports.
    pub fn port_count(&self) -> u16 {
        // SAFETY: EAL is initialized (we hold `self`).
        unsafe { ffi::rte_eth_dev_count_avail() }
    }
}

impl Drop for Eal {
    fn drop(&mut self) {
        // SAFETY: EAL was initialized in `init()`. Cleanup is called once.
        unsafe {
            ffi::rte_eal_cleanup();
        }
        tracing::info!("DPDK EAL cleaned up");
    }
}

/// Errors from EAL initialization.
#[derive(Debug)]
pub enum EalError {
    /// An argument contained a null byte.
    InvalidArg,
    /// `rte_eal_init` returned an error code.
    InitFailed(libc::c_int),
}

impl std::fmt::Display for EalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EalError::InvalidArg => write!(f, "EAL argument contains null byte"),
            EalError::InitFailed(code) => write!(f, "rte_eal_init failed with code {code}"),
        }
    }
}

impl std::error::Error for EalError {}
