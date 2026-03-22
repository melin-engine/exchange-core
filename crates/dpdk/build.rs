//! Build script for DPDK FFI bindings.
//!
//! Uses pkg-config to locate the DPDK installation, then bindgen to generate
//! Rust FFI bindings from the DPDK C headers. Requires DPDK >= 22.11 installed
//! and discoverable via `pkg-config --cflags --libs libdpdk`.

fn main() {
    // Locate DPDK via pkg-config. This provides compiler flags (-I paths)
    // and linker flags (-L paths, -l libs) for the installed DPDK version.
    let dpdk = pkg_config::Config::new()
        .atleast_version("22.11")
        .probe("libdpdk")
        .expect(
            "DPDK not found. Install DPDK >= 22.11 and ensure pkg-config can find it.\n\
             On Fedora: dnf install dpdk-devel\n\
             On Ubuntu: apt install libdpdk-dev\n\
             Verify: pkg-config --cflags --libs libdpdk",
        );

    // Collect include paths for bindgen.
    let include_args: Vec<String> = dpdk
        .include_paths
        .iter()
        .map(|p| format!("-I{}", p.display()))
        .collect();

    // Generate bindings for the subset of DPDK we need:
    // - EAL (rte_eal_init, rte_eal_cleanup)
    // - Mempool (rte_pktmbuf_pool_create, rte_pktmbuf_alloc, rte_pktmbuf_free)
    // - Ethernet device (rte_eth_dev_configure, rte_eth_rx/tx_queue_setup,
    //   rte_eth_dev_start/stop, rte_eth_rx/tx_burst)
    // - Mbuf (rte_mbuf, rte_pktmbuf_mtod, data_len, pkt_len)
    let bindings = bindgen::Builder::default()
        .header_contents(
            "dpdk_wrapper.h",
            "\
            #include <rte_eal.h>\n\
            #include <rte_ethdev.h>\n\
            #include <rte_mbuf.h>\n\
            #include <rte_mempool.h>\n\
            #include <rte_lcore.h>\n\
            ",
        )
        .clang_args(&include_args)
        // Only generate bindings for rte_* symbols we actually use.
        .allowlist_function("rte_eal_init")
        .allowlist_function("rte_eal_cleanup")
        .allowlist_function("rte_pktmbuf_pool_create")
        .allowlist_function("rte_pktmbuf_alloc")
        .allowlist_function("rte_pktmbuf_free")
        .allowlist_function("rte_eth_dev_configure")
        .allowlist_function("rte_eth_dev_count_avail")
        .allowlist_function("rte_eth_dev_info_get")
        .allowlist_function("rte_eth_dev_start")
        .allowlist_function("rte_eth_dev_stop")
        .allowlist_function("rte_eth_rx_queue_setup")
        .allowlist_function("rte_eth_tx_queue_setup")
        .allowlist_function("rte_eth_rx_burst")
        .allowlist_function("rte_eth_tx_burst")
        .allowlist_function("rte_eth_dev_socket_id")
        .allowlist_function("rte_eth_macaddr_get")
        .allowlist_function("rte_eth_promiscuous_enable")
        .allowlist_function("rte_eth_link_get_nowait")
        .allowlist_function("rte_pktmbuf_mtod")
        .allowlist_function("rte_socket_id")
        // Types we need for struct access.
        .allowlist_type("rte_mbuf")
        .allowlist_type("rte_mempool")
        .allowlist_type("rte_eth_conf")
        .allowlist_type("rte_eth_dev_info")
        .allowlist_type("rte_eth_link")
        .allowlist_type("rte_eth_rxconf")
        .allowlist_type("rte_eth_txconf")
        .allowlist_type("rte_ether_addr")
        // Derive traits for FFI types.
        .derive_default(true)
        .derive_debug(true)
        // Use core types where possible (no_std friendly).
        .use_core()
        .generate()
        .expect("failed to generate DPDK bindings");

    let out_path = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("dpdk_bindings.rs"))
        .expect("failed to write dpdk_bindings.rs");
}
