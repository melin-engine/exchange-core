#!/usr/bin/env bash
# DPDK net_tap replication FEASIBILITY PROBE: kernel-TCP primary + DPDK replica.
#
# Unlike dpdk-replication-smoke-test.sh (which is DPDK<->DPDK over veth +
# af_packet), this probe answers the one question the net_tap test harness
# depends on:
#
#   Can a smoltcp-over-DPDK replica using `--vdev=net_tap0` open a TCP
#   connection to a *kernel-stack* replication primary across the TAP bridge,
#   and complete a happy-path sync?
#
# If yes, a lightweight kernel-TCP *scripted* primary can later drive a real
# DPDK replica through adversarial divergence/resync scenarios (Phase 2). If
# no, the whole net_tap approach is infeasible and we learn it cheaply, here,
# with zero new Rust code.
#
# Topology:
#
#     kernel-TCP primary (default build)          DPDK replica (dpdk build)
#     binds 0.0.0.0:9877  <----- mlntap0 ----->   smoltcp 192.168.222.2
#     reachable via 192.168.222.1               --replica-of 192.168.222.1:9877
#                          (kernel)                       --vdev=net_tap0
#
# DPDK's tap PMD creates the kernel-side interface `mlntap0`; we give it the
# primary-side IP so the replica's on-subnet SYN routes to the kernel listener.
#
# Usage:
#   sudo ./scripts/dpdk/dpdk-tap-replication-smoke-test.sh
#
# Prerequisites:
#   - DPDK >= 22.11 installed (tap PMD)
#   - Must run as root (hugepages + TAP interface configuration)

set -euo pipefail

# Ensure cargo/rustup work when running under sudo.
if [[ -n "${SUDO_USER:-}" ]]; then
    REAL_HOME=$(eval echo "~$SUDO_USER")
    export PATH="$REAL_HOME/.cargo/bin:$PATH"
    export RUSTUP_HOME="${RUSTUP_HOME:-$REAL_HOME/.rustup}"
    export CARGO_HOME="${CARGO_HOME:-$REAL_HOME/.cargo}"
fi

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root (hugepages + TAP configuration)" >&2
    echo "usage: sudo $0" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
TMPDIR=$(mktemp -d)

# IP configuration: kernel/primary side .1, replica smoltcp side .2, same /24.
PRIMARY_IP="192.168.222.1"
REPLICA_IP="192.168.222.2"
PREFIX=24
REPL_PORT=9877
HEALTH_PORT=9878

# Kernel-side interface name the DPDK tap PMD will create (net_tap0,iface=...).
TAP_IFACE="mlntap0"

# Capture core dumps to a temp file (bypassing apport) so we can pull a
# backtrace if the replica crashes. Restored in cleanup.
ORIG_CORE_PATTERN=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || echo "core")
ulimit -c unlimited
echo "$TMPDIR/core.%e.%p" > /proc/sys/kernel/core_pattern 2>/dev/null || true

cleanup() {
    echo ""
    echo "=== Cleanup ==="

    if [[ -n "${TCPDUMP_PID:-}" ]]; then
        kill "$TCPDUMP_PID" 2>/dev/null || true
    fi
    if [[ -n "${REPLICA_PID:-}" ]]; then
        kill "$REPLICA_PID" 2>/dev/null && wait "$REPLICA_PID" 2>/dev/null || true
        echo "  Replica stopped"
    fi
    if [[ -n "${PRIMARY_PID:-}" ]]; then
        kill "$PRIMARY_PID" 2>/dev/null && wait "$PRIMARY_PID" 2>/dev/null || true
        echo "  Primary stopped"
    fi

    # The tap PMD removes mlntap0 when the replica exits, but delete it
    # defensively in case the replica died mid-startup.
    ip link del "$TAP_IFACE" 2>/dev/null || true

    rm -rf /var/run/dpdk/replica

    # Restore the original core_pattern.
    echo "$ORIG_CORE_PATTERN" > /proc/sys/kernel/core_pattern 2>/dev/null || true

    if [[ "${MOUNTED_HUGE_2M:-}" == "1" ]]; then
        umount "$HUGE_2M_MOUNT" 2>/dev/null || true
    fi

    if [[ -n "${SUDO_USER:-}" ]]; then
        chown -R "$SUDO_USER:$SUDO_USER" "$PROJECT_DIR/target" 2>/dev/null || true
        echo "  Restored target/ ownership to $SUDO_USER"
    fi

    rm -rf "$TMPDIR"
    echo "  Temp dir cleaned: $TMPDIR"
}
trap cleanup EXIT

echo "============================================================"
echo "  DPDK net_tap Replication Probe (kernel primary + DPDK replica)"
echo "  Primary: kernel-TCP, listens :$REPL_PORT, reachable via $PRIMARY_IP"
echo "  Replica: DPDK net_tap, smoltcp $REPLICA_IP --replica-of $PRIMARY_IP"
echo "  TAP iface (kernel side): $TAP_IFACE"
echo "  Temp:    $TMPDIR"
echo "============================================================"
echo ""

# --- 0. Clean stale DPDK state ---
rm -rf /var/run/dpdk/replica
ip link del "$TAP_IFACE" 2>/dev/null || true

# --- 1. Hugepages ---
echo "=== Hugepages ==="
HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages 2>/dev/null || echo 0)
if [[ "$HUGEPAGE_COUNT" -lt 512 ]]; then
    echo "  Allocating 512 x 2MB hugepages..."
    echo 512 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
    HUGEPAGE_COUNT=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
fi
echo "  Hugepages available: $HUGEPAGE_COUNT x 2MB"

HUGE_2M_MOUNT="/mnt/huge_2m"
if ! mount | grep -q "pagesize=2M"; then
    mkdir -p "$HUGE_2M_MOUNT"
    mount -t hugetlbfs -o pagesize=2M nodev "$HUGE_2M_MOUNT"
    MOUNTED_HUGE_2M=1
    echo "  Mounted 2MB hugetlbfs at $HUGE_2M_MOUNT"
else
    HUGE_2M_MOUNT=$(mount | grep "pagesize=2M" | awk '{print $3}' | head -1)
    echo "  2MB hugetlbfs already mounted at $HUGE_2M_MOUNT"
fi
echo ""

# --- 2. Build ---
# The kernel primary and the DPDK replica are the SAME crate built with
# different features, so they collide at target/release/melin-server. Build
# the default (kernel-TCP) server first and stash it, then build the DPDK
# server in place for the replica.
echo "=== Building ==="
cd "$PROJECT_DIR"

echo "  Building keygen..."
cargo build --release --bin melin-keygen --quiet 2>&1
echo "  keygen: OK"

echo "  Building kernel-TCP server (default features)..."
cargo build --release -p melin-server --quiet 2>&1
cp "$PROJECT_DIR/target/release/melin-server" "$TMPDIR/melin-server-kernel"
echo "  kernel server: OK (stashed)"

echo "  Building DPDK server..."
cargo build --release -p melin-server --features dpdk --no-default-features --quiet 2>&1
echo "  dpdk server: OK"
echo ""

# --- 3. Auth keys ---
echo "=== Auth keys ==="
cd "$TMPDIR"
"$PROJECT_DIR/target/release/melin-keygen" repl_key trader
echo "trader $(cat repl_key.pub | tr -d '\n') repl" > authorized_keys
echo "  Generated repl_key.key + authorized_keys"
echo ""

# --- 4. Start DPDK replica (creates the kernel TAP interface) ---
# Start the replica first: the tap PMD creates mlntap0 only once EAL probes
# the vdev. The replica then retries its outbound connection (refused until
# the primary is up), which gives us a window to configure the kernel side.
echo "=== Starting DPDK net_tap replica ==="
RUST_LOG=info \
"$PROJECT_DIR/target/release/melin-server" \
    --journal "$TMPDIR/replica.journal" \
    --snapshot-interval-ms 0 \
    --replica-of "$PRIMARY_IP:$REPL_PORT" \
    --replication-key "$TMPDIR/repl_key.key" \
    --dpdk-eal-args="--vdev=net_tap0,iface=$TAP_IFACE --no-pci --log-level=6 --huge-dir=$HUGE_2M_MOUNT --file-prefix=replica" \
    --dpdk-ip "$REPLICA_IP" \
    --dpdk-prefix-len "$PREFIX" \
    > "$TMPDIR/replica.log" 2>&1 &
REPLICA_PID=$!
echo "  Replica PID: $REPLICA_PID"

# --- 5. Wait for the TAP interface to appear, then configure the kernel side ---
echo "  Waiting for kernel TAP interface $TAP_IFACE..."
WAIT=0
while ! ip link show "$TAP_IFACE" >/dev/null 2>&1; do
    sleep 0.5
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 30 ]]; then
        echo "  ERROR: $TAP_IFACE never appeared after 15s"
        echo "  --- Replica log ---"
        cat "$TMPDIR/replica.log"
        exit 1
    fi
    if ! kill -0 "$REPLICA_PID" 2>/dev/null; then
        echo "  ERROR: Replica died before creating $TAP_IFACE"
        echo "  --- Replica log ---"
        cat "$TMPDIR/replica.log"
        exit 1
    fi
done

# Give the kernel side the primary IP so the replica's on-subnet SYN routes
# here. Force software checksums (the tap PMD path has no offload).
# The DPDK replica skips ARP and seeds the primary's MAC as 02:00:<ip-octets>
# (the SR-IOV VF convention from dpdk-setup.sh; see replication/dpdk.rs). The
# kernel TAP interface stands in for that VF, so it must OWN that MAC or it
# drops every frame the replica sends. fastcp's own source MAC stays the tap's
# real MAC, so the kernel's ARP for the replica resolves normally on the way
# back.
IFS=. read -r o1 o2 o3 o4 <<< "$PRIMARY_IP"
SYNTH_MAC=$(printf '02:00:%02x:%02x:%02x:%02x' "$o1" "$o2" "$o3" "$o4")
ip link set dev "$TAP_IFACE" down
ip link set dev "$TAP_IFACE" address "$SYNTH_MAC"
ip addr add "$PRIMARY_IP/$PREFIX" dev "$TAP_IFACE"
ip link set dev "$TAP_IFACE" up
ethtool -K "$TAP_IFACE" tx off rx off 2>/dev/null || true
# Symmetric path on one interface — keep rp_filter from dropping replica frames.
# Effective rp_filter is max(all, iface), so clear all/default too.
sysctl -qw "net.ipv4.conf.$TAP_IFACE.rp_filter=0" 2>/dev/null || true
sysctl -qw "net.ipv4.conf.all.rp_filter=0" 2>/dev/null || true
sysctl -qw "net.ipv4.conf.default.rp_filter=0" 2>/dev/null || true
TAP_MAC=$(cat "/sys/class/net/$TAP_IFACE/address" 2>/dev/null || echo "?")
echo "  Configured $TAP_IFACE: $PRIMARY_IP/$PREFIX (up), MAC $TAP_MAC (= replica's seeded peer MAC)"

# Capture the bridge so we can see, on failure, whether ARP resolves, whether
# the replica's SYNs arrive, and whether their IP/TCP checksums are accepted
# (tcpdump prints "incorrect -> 0x..." for a bad checksum the kernel will drop).
TCPDUMP_LOG="$TMPDIR/tap.tcpdump"
if command -v tcpdump >/dev/null 2>&1; then
    # -e shows L2 MACs so we can see whether the replica's SYN is addressed
    # to the kernel TAP MAC or somewhere else.
    tcpdump -i "$TAP_IFACE" -e -nnvv -c 200 'arp or icmp or tcp port '"$REPL_PORT" \
        > "$TCPDUMP_LOG" 2>&1 &
    TCPDUMP_PID=$!
    echo "  tcpdump capturing $TAP_IFACE -> $TCPDUMP_LOG (PID $TCPDUMP_PID)"
else
    echo "  WARN: tcpdump not installed — skipping packet capture"
fi
echo ""

# --- 5b. L2 connectivity probe (host kernel -> replica smoltcp) ---
# smoltcp always answers ARP for its own IP, so an arping reply proves the full
# kernel -> DPDK(tx) -> smoltcp -> DPDK(rx) -> kernel round trip works and the
# problem (if any) is at L3/socket. No reply means the RX direction or L2 is
# broken.
echo "=== L2 probe: arping replica smoltcp ($REPLICA_IP) ==="
if command -v arping >/dev/null 2>&1; then
    arping -c 3 -w 3 -I "$TAP_IFACE" "$REPLICA_IP" || \
        echo "  arping: NO REPLY — kernel->DPDK->smoltcp (RX) or L2 is broken"
else
    echo "  (arping not installed: apt-get install iputils-arping — skipping)"
fi
echo ""

# --- 6. Start kernel-TCP primary ---
echo "=== Starting kernel-TCP primary ==="
RUST_LOG=info \
"$TMPDIR/melin-server-kernel" \
    --bind "0.0.0.0:9876" \
    --health-bind "0.0.0.0:$HEALTH_PORT" \
    --journal "$TMPDIR/primary.journal" \
    --authorized-keys "$TMPDIR/authorized_keys" \
    --accounts 100 \
    --instruments 10 \
    --replication-bind "0.0.0.0:$REPL_PORT" \
    > "$TMPDIR/primary.log" 2>&1 &
PRIMARY_PID=$!
echo "  Primary PID: $PRIMARY_PID"
echo ""

# --- 7. Wait for the replica to connect and start streaming ---
echo "=== Waiting for replica to sync over net_tap ==="
# The replica's first connect attempt starts ~0.8s after it boots — before the
# primary is listening — and burns its full 10s timeout. The first attempt that
# can actually reach the primary is the second one (~T+11s). Wait long enough to
# cover several attempts so a slow-but-working setup isn't mistaken for failure.
WAIT=0
while ! grep -q "streaming started (DPDK)" "$TMPDIR/replica.log" 2>/dev/null; do
    sleep 0.5
    WAIT=$((WAIT + 1))
    if [[ $WAIT -gt 70 ]]; then
        echo "  ERROR: Replica not streaming after 35s"
        echo "  --- Replica log ---"
        cat "$TMPDIR/replica.log"
        echo "  --- Primary log (last 30 lines) ---"
        tail -30 "$TMPDIR/primary.log"
        echo "  --- Network state ---"
        echo "  TAP $TAP_IFACE kernel MAC: ${TAP_MAC:-?}"
        ip -d link show "$TAP_IFACE" 2>/dev/null || true
        ip -4 addr show "$TAP_IFACE" 2>/dev/null || true
        echo "  neigh:"; ip neigh show dev "$TAP_IFACE" 2>/dev/null || true
        echo "  route to $REPLICA_IP:"; ip route get "$REPLICA_IP" 2>/dev/null || true
        echo "  rp_filter: all=$(cat /proc/sys/net/ipv4/conf/all/rp_filter 2>/dev/null) iface=$(cat /proc/sys/net/ipv4/conf/$TAP_IFACE/rp_filter 2>/dev/null)"
        echo "  --- TAP capture ($TAP_IFACE) ---"
        [[ -n "${TCPDUMP_PID:-}" ]] && kill "$TCPDUMP_PID" 2>/dev/null && sleep 0.3
        cat "$TCPDUMP_LOG" 2>/dev/null || echo "  (no capture)"
        exit 1
    fi
    for PID in "$REPLICA_PID" "$PRIMARY_PID"; do
        if ! kill -0 "$PID" 2>/dev/null; then
            echo "  ERROR: process $PID died"
            echo "  --- Replica log ---"
            cat "$TMPDIR/replica.log"
            echo "  --- Primary log ---"
            cat "$TMPDIR/primary.log"
            # If a core was dumped, pull a backtrace (release build keeps
            # symbol names, so frame functions are identifiable).
            CORE=$(ls -1t "$TMPDIR"/core.* 2>/dev/null | head -1)
            if [[ -n "$CORE" ]] && command -v gdb >/dev/null 2>&1; then
                echo "  --- Crash backtrace ($CORE) ---"
                gdb -batch -nx \
                    -ex 'bt' \
                    -ex 'echo \n=== all threads ===\n' \
                    -ex 'thread apply all bt' \
                    "$PROJECT_DIR/target/release/melin-server" "$CORE" 2>&1 \
                    | grep -vE '^(\[New|warning: |Reading symbols|\[Thread)' | head -80
            else
                echo "  (no core dump found in $TMPDIR)"
            fi
            exit 1
        fi
    done
done
echo "  Replica connected and streaming over net_tap"

# Give the replica a moment to receive seeded data.
sleep 2

# --- 8. Verify ---
echo ""
echo "=== Verification ==="
PASSED=true

HEALTH=$(echo "" | nc -q1 127.0.0.1 "$HEALTH_PORT" 2>/dev/null || true)
echo "  Primary health: $HEALTH"
if echo "$HEALTH" | grep -q "trading"; then
    echo "  PASS: primary is trading (replica connected)"
else
    echo "  FAIL: expected 'trading' status"
    PASSED=false
fi

if [[ -f "$TMPDIR/replica.journal" ]]; then
    REPLICA_SIZE=$(stat -c%s "$TMPDIR/replica.journal")
    echo "  Replica journal: $REPLICA_SIZE bytes"
    if [[ "$REPLICA_SIZE" -gt 100 ]]; then
        echo "  PASS: replica journal has data (kernel->TAP->smoltcp worked)"
    else
        echo "  FAIL: replica journal too small ($REPLICA_SIZE bytes)"
        PASSED=false
    fi
else
    echo "  FAIL: replica journal not found"
    PASSED=false
fi

if kill -0 "$PRIMARY_PID" 2>/dev/null; then
    echo "  PASS: primary still running"
else
    echo "  FAIL: primary died"
    PASSED=false
fi

if kill -0 "$REPLICA_PID" 2>/dev/null; then
    echo "  PASS: replica still running"
else
    echo "  FAIL: replica died"
    PASSED=false
fi

echo ""
if [[ "$PASSED" == "true" ]]; then
    echo "============================================================"
    echo "  DPDK net_tap REPLICATION PROBE: PASSED"
    echo "  -> kernel-TCP primary <-> DPDK net_tap replica is viable;"
    echo "     Phase 2 (scripted adversarial primary) can proceed."
    echo "============================================================"
else
    echo "============================================================"
    echo "  DPDK net_tap REPLICATION PROBE: FAILED"
    echo "============================================================"
    echo ""
    echo "  --- Replica log (last 40 lines) ---"
    tail -40 "$TMPDIR/replica.log"
    echo ""
    echo "  --- Primary log (last 30 lines) ---"
    tail -30 "$TMPDIR/primary.log"
    exit 1
fi
