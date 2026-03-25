#!/usr/bin/env bash
# Set up DPDK with a standalone NIC (no bond).
#
# For machines with two NICs where one has the public/VLAN IP and the
# other is free (e.g., eno1 = management, eno2 = unused). Binds the
# free NIC to vfio-pci for DPDK. No bond teardown needed.
#
# Prerequisites:
#   - A free NIC not carrying any traffic (no IP, not in a bond)
#   - IOMMU enabled (iommu=pt in kernel cmdline)
#   - Root access
#
# Usage:
#   ./scripts/dpdk-setup-standalone.sh [--iface eno2] [--vlan 2060] [--ip 10.188.77.115/24]
#
# The script auto-detects:
#   - Free NIC: first ixgbe/i40e/ice interface with no IP address
#   - VLAN ID: from any .XXXX VLAN subinterface on the primary NIC
#   - DPDK IP: primary VLAN IP + 100 in the last octet

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DPDK_IFACE=""
VLAN_ID=""
DPDK_IP="${DPDK_IP:-auto}"
HUGEPAGES="${HUGEPAGES:-1024}"
MTU="${MTU:-1500}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --iface) DPDK_IFACE="$2"; shift 2 ;;
        --vlan) VLAN_ID="$2"; shift 2 ;;
        --ip) DPDK_IP="$2"; shift 2 ;;
        --mtu) MTU="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ $EUID -ne 0 ]]; then
    echo "error: must run as root" >&2
    exit 1
fi

export CARGO_NET_GIT_FETCH_WITH_CLI=true
if ! grep -q "CARGO_NET_GIT_FETCH_WITH_CLI" /etc/environment 2>/dev/null; then
    echo 'CARGO_NET_GIT_FETCH_WITH_CLI=true' >> /etc/environment
fi

# ---------------------------------------------------------------------------
# Auto-detect free NIC
# ---------------------------------------------------------------------------

if [[ -z "$DPDK_IFACE" ]]; then
    # Find a NIC with no IP and not in a bond — likely the free second port.
    for dev in /sys/class/net/*; do
        iface=$(basename "$dev")
        # Skip loopback, bonds, VLANs, virtual interfaces.
        [[ "$iface" == lo ]] && continue
        [[ "$iface" == bond* ]] && continue
        [[ "$iface" == *"."* ]] && continue
        [[ ! -e "$dev/device" ]] && continue
        # Skip if it has an IP.
        if ip -4 addr show "$iface" 2>/dev/null | grep -q 'inet '; then
            continue
        fi
        # Skip if it's a bond slave.
        if [[ -e "$dev/master" ]]; then
            continue
        fi
        # Check driver — only physical NICs.
        driver=$(ethtool -i "$iface" 2>/dev/null | grep driver | awk '{print $2}')
        case "$driver" in
            ixgbe|i40e|ice|igb|mlx4_en|mlx5_core) ;;
            *) continue ;;
        esac
        DPDK_IFACE="$iface"
        break
    done
fi

if [[ -z "$DPDK_IFACE" ]]; then
    echo "error: could not find a free NIC for DPDK" >&2
    echo "  Use --iface <name> to specify one manually" >&2
    exit 1
fi

DPDK_PCI=$(ethtool -i "$DPDK_IFACE" 2>/dev/null | grep bus-info | awk '{print $2}')
if [[ -z "$DPDK_PCI" ]]; then
    echo "error: could not determine PCI address for $DPDK_IFACE" >&2
    exit 1
fi

# Find the primary NIC (the one with an IP).
PRIMARY_IFACE=""
for dev in /sys/class/net/*; do
    iface=$(basename "$dev")
    [[ "$iface" == lo ]] && continue
    [[ "$iface" == *"."* ]] && continue
    [[ "$iface" == "$DPDK_IFACE" ]] && continue
    if ip -4 addr show "$iface" 2>/dev/null | grep -q 'inet '; then
        PRIMARY_IFACE="$iface"
        break
    fi
done

# ---------------------------------------------------------------------------
# Auto-detect VLAN ID
# ---------------------------------------------------------------------------

if [[ -z "$VLAN_ID" ]]; then
    # Look for a VLAN subinterface on any NIC.
    VLAN_IFACE=$(ip -o link show | grep -oP '\b\w+\.\d+' | head -1)
    if [[ -n "$VLAN_IFACE" ]]; then
        VLAN_ID="${VLAN_IFACE##*.}"
    else
        echo "error: could not auto-detect VLAN ID" >&2
        echo "  Use --vlan <id> to specify one manually" >&2
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# Auto-detect DPDK IP
# ---------------------------------------------------------------------------

if [[ "$DPDK_IP" == "auto" ]]; then
    # Find the VLAN subinterface IP.
    VLAN_IFACE=$(ip -o link show | grep -oP "\b\w+\.${VLAN_ID}\b" | head -1)
    if [[ -z "$VLAN_IFACE" ]]; then
        echo "error: no VLAN interface found for VLAN $VLAN_ID" >&2
        exit 1
    fi
    VLAN_IP=$(ip -4 addr show "$VLAN_IFACE" 2>/dev/null | grep -oP 'inet \K[\d.]+')
    VLAN_PREFIX=$(ip -4 addr show "$VLAN_IFACE" 2>/dev/null | grep -oP 'inet [\d.]+/\K\d+')

    if [[ -z "$VLAN_IP" ]]; then
        echo "error: could not detect IP on $VLAN_IFACE — use --ip manually" >&2
        exit 1
    fi

    IFS='.' read -r a b c d <<< "$VLAN_IP"
    DPDK_LAST=$(( (d + 100) % 256 ))
    [[ "$DPDK_LAST" -eq "$d" ]] && DPDK_LAST=$(( (d + 101) % 256 ))
    DPDK_IP="${a}.${b}.${c}.${DPDK_LAST}/${VLAN_PREFIX}"

    echo "  VLAN IP:  ${VLAN_IP}/${VLAN_PREFIX} (${VLAN_IFACE})"
    echo "  DPDK IP:  ${DPDK_IP} (auto-derived)"
fi

echo "=== DPDK Standalone NIC Setup ==="
echo "  DPDK NIC:  ${DPDK_IFACE} (${DPDK_PCI})"
echo "  Primary:   ${PRIMARY_IFACE:-unknown} (SSH/management)"
echo "  VLAN:      ${VLAN_ID}"
echo "  DPDK IP:   ${DPDK_IP}"
echo "  MTU:       ${MTU}"
echo ""

# ---------------------------------------------------------------------------
# 1. Configure hugepages
# ---------------------------------------------------------------------------

echo "--- Configuring hugepages (${HUGEPAGES} x 2MB) ---"

echo "$HUGEPAGES" > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

mkdir -p /mnt/huge_2m
if ! mount | grep -q "/mnt/huge_2m"; then
    mount -t hugetlbfs -o pagesize=2M nodev /mnt/huge_2m
    echo "  Mounted 2MB hugetlbfs at /mnt/huge_2m"
fi
if ! grep -q "/mnt/huge_2m" /etc/fstab 2>/dev/null; then
    echo "nodev /mnt/huge_2m hugetlbfs pagesize=2M 0 0" >> /etc/fstab
    echo "  Added /mnt/huge_2m to /etc/fstab"
fi

ACTUAL=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
echo "  Hugepages allocated: ${ACTUAL}"

# ---------------------------------------------------------------------------
# 2. Load vfio-pci module
# ---------------------------------------------------------------------------

echo ""
echo "--- Loading vfio-pci module ---"

modprobe vfio-pci
echo "  vfio-pci loaded"

if [[ -f /sys/module/vfio/parameters/enable_unsafe_noiommu_mode ]]; then
    echo 1 > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 3. Bind NIC to DPDK
# ---------------------------------------------------------------------------

echo ""
echo "--- Binding ${DPDK_IFACE} (${DPDK_PCI}) to vfio-pci ---"

ip link set "$DPDK_IFACE" down 2>/dev/null || true

if [[ -e "/sys/bus/pci/devices/${DPDK_PCI}/driver" ]]; then
    echo "${DPDK_PCI}" > "/sys/bus/pci/devices/${DPDK_PCI}/driver/unbind" 2>/dev/null || true
    echo "  Unbound from kernel driver"
fi

local_vendor=$(cat "/sys/bus/pci/devices/${DPDK_PCI}/vendor")
local_device=$(cat "/sys/bus/pci/devices/${DPDK_PCI}/device")
echo "${local_vendor} ${local_device}" > /sys/bus/pci/drivers/vfio-pci/new_id 2>/dev/null || true
echo "${DPDK_PCI}" > /sys/bus/pci/drivers/vfio-pci/bind 2>/dev/null || true
echo "  Bound ${DPDK_PCI} to vfio-pci"

# ---------------------------------------------------------------------------
# 4. Verify and save config
# ---------------------------------------------------------------------------

echo ""
echo "--- Verification ---"

echo "  Hugepages:"
grep -i huge /proc/meminfo | head -3

echo ""
echo "  DPDK-bound devices:"
ls -la /sys/bus/pci/drivers/vfio-pci/ 2>/dev/null | grep "0000:" || echo "  (none found)"

DPDK_CONF="/etc/melin-dpdk.conf"
cat > "$DPDK_CONF" <<EOF
DPDK_IP=${DPDK_IP%%/*}
DPDK_PREFIX=${DPDK_IP##*/}
DPDK_PORT=0
DPDK_PCI=${DPDK_PCI}
DPDK_MODE=standalone
HUGE_DIR=/mnt/huge_2m
MTU=${MTU}
DPDK_IFACE=${DPDK_IFACE}
VLAN_ID=${VLAN_ID}
EOF
echo "  Config written to ${DPDK_CONF}"

echo ""
echo "=== Setup complete ==="
echo ""
echo "  ${DPDK_IFACE} (${DPDK_PCI}) → vfio-pci"
echo "  ${PRIMARY_IFACE:-?} → kernel (SSH/management)"
echo "  Hugepages: ${ACTUAL} x 2MB"
echo ""
echo "  Start the server with:"
echo "    ./target/release/melin-server \\"
echo "      --dpdk-eal-args='--huge-dir=/mnt/huge_2m' \\"
echo "      --dpdk-ports 0 \\"
echo "      --dpdk-ip ${DPDK_IP%%/*} \\"
echo "      --dpdk-prefix-len ${DPDK_IP##*/} \\"
echo "      --dpdk-mtu ${MTU} \\"
echo "      --dpdk-vlan ${VLAN_ID} \\"
echo "      --journal /mnt/journal/bench.journal \\"
echo "      --authorized-keys authorized_keys \\"
echo "      --standalone --busy-spin"
echo ""
echo "  To restore: reboot, or run:"
echo "    echo ${DPDK_PCI} > /sys/bus/pci/drivers/vfio-pci/unbind"
echo "    echo 1 > /sys/bus/pci/devices/${DPDK_PCI}/driver_override"
echo "    echo ${DPDK_PCI} > /sys/bus/pci/drivers/ixgbe/bind"
