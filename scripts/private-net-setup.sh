#!/usr/bin/env bash
# Configure a Latitude.sh private-network VLAN on this host.
#
# Latitude connects the second NIC port of each server to a customer VLAN,
# but ships it unconfigured: the port is link-up and silent until the OS
# is told to tag frames. On bare metal the traffic must carry an 802.1Q
# tag; the addresses inside the VLAN are ours to choose.
#
# Run as root, on the server (either directly or via sudo).
#
# Usage:
#   sudo ./scripts/private-net-setup.sh --vlan-id 2025 --address 10.8.0.1/24
#   sudo ./scripts/private-net-setup.sh --vlan-id 2025 --address 10.8.0.1/24 --peer 10.8.0.2
#   sudo ./scripts/private-net-setup.sh --vlan-id 2025 --address 10.8.0.1/24 --mtu 9000
#   sudo ./scripts/private-net-setup.sh --down --vlan-id 2025
#
# Options:
#   --vlan-id <N>       802.1Q tag, 1-4094. This is the numeric VLAN ID from
#                       the Latitude dashboard, not the `vlan_xxx` resource id.
#   --address <CIDR>    This host's address inside the VLAN. Every host in the
#                       VLAN needs a different one; the prefix must match.
#   --link <IFACE>      Parent interface (default: eno2, Latitude's private port).
#   --mtu <N>           MTU for the VLAN interface. The parent is raised to
#                       N+4 to leave room for the 802.1Q tag. Only set this if
#                       the switch passes jumbo frames — if it does not, large
#                       packets vanish silently rather than erroring.
#   --peer <IP>         After configuring, prove the link by pinging this
#                       address. Without it the script can only confirm the
#                       local half.
#   --down              Tear down the VLAN interface and remove the persisted
#                       config. Needs --vlan-id (and --link if not the default).
#
# The live interface is created with `ip`, deliberately, rather than by
# running `netplan apply`: apply re-applies every interface on the host,
# including the one carrying our SSH session. A surgical `ip link add`
# cannot drop the management link. The netplan file is still written, so
# the config survives a reboot, and is validated with `netplan generate`
# before we trust it.

set -euo pipefail

LINK="eno2"
VLAN_ID=""
ADDRESS=""
PEER=""
MTU=""
MODE="up"

NETPLAN_DIR="/etc/netplan"

die() {
    echo "error: $*" >&2
    exit 1
}

# Block until `iface` reports carrier, up to `timeout` seconds. Changing the
# MTU on an ixgbe port resets the adapter, and on 10GBASE-T copper the PHY
# then has to retrain — measured at over 15s on an X550, occasionally longer.
# Verifying through that window reports a dead peer and sends whoever is
# reading the output hunting for a VLAN misconfiguration that isn't there,
# so the timeout is generous on purpose.
wait_for_carrier() {
    local iface="$1" timeout="${2:-60}" waited=0
    while (( waited < timeout )); do
        if [[ "$(cat "/sys/class/net/${iface}/carrier" 2>/dev/null || echo 0)" == "1" ]]; then
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
    return 1
}

# Ping with retries. Same reasoning as wait_for_carrier: the first probe
# after a reconfigure can land before the link is forwarding again, and a
# single failed shot is not evidence the network is broken.
ping_peer() {
    local iface="$1" peer="$2" extra="${3:-}" deadline=$((SECONDS + 20))
    while (( SECONDS < deadline )); do
        # shellcheck disable=SC2086
        if ping -c 2 -W 2 $extra -I "$iface" "$peer" &>/dev/null; then
            return 0
        fi
        sleep 2
    done
    return 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --vlan-id) VLAN_ID="${2:-}"; shift 2 ;;
        --address) ADDRESS="${2:-}"; shift 2 ;;
        --link)    LINK="${2:-}"; shift 2 ;;
        --peer)    PEER="${2:-}"; shift 2 ;;
        --mtu)     MTU="${2:-}"; shift 2 ;;
        --down)    MODE="down"; shift ;;
        -h|--help)
            sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) die "unknown argument: $1 (try --help)" ;;
    esac
done

# -------------------------------------------------------------------------
# Validation. Everything that can be checked before touching the network is
# checked here: on a rented bench host, a script that refuses to start costs
# nothing, while one that half-applies costs a debugging session.
#
# Argument checks run before the root check on purpose — a mistyped VLAN id
# should be named whether or not the caller remembered sudo.
# -------------------------------------------------------------------------
[[ -n "$VLAN_ID" ]] || die "--vlan-id is required"
[[ "$VLAN_ID" =~ ^[0-9]+$ ]] || die "--vlan-id must be a number, got '$VLAN_ID'. \
Latitude's dashboard also shows a 'vlan_xxx' resource id — that is not the tag."
if (( VLAN_ID < 1 || VLAN_ID > 4094 )); then
    die "--vlan-id must be 1-4094, got $VLAN_ID"
fi

VLAN_IF="${LINK}.${VLAN_ID}"
NETPLAN_FILE="${NETPLAN_DIR}/60-melin-private-${VLAN_ID}.yaml"

if [[ $EUID -ne 0 ]]; then
    die "must run as root"
fi

if [[ "$MODE" == "down" ]]; then
    echo "=== Tearing down ${VLAN_IF} ==="
    if ip link show "$VLAN_IF" &>/dev/null; then
        ip link del "$VLAN_IF"
        echo "  removed interface ${VLAN_IF}"
    else
        echo "  interface ${VLAN_IF} not present"
    fi
    if [[ -f "$NETPLAN_FILE" ]]; then
        rm -f "$NETPLAN_FILE"
        echo "  removed ${NETPLAN_FILE}"
    else
        echo "  ${NETPLAN_FILE} not present"
    fi
    echo "=== Done ==="
    exit 0
fi

[[ -n "$ADDRESS" ]] || die "--address is required (e.g. 10.8.0.1/24)"
[[ "$ADDRESS" == */* ]] || die "--address must include a prefix length, e.g. ${ADDRESS}/24"

if [[ -n "$MTU" ]]; then
    [[ "$MTU" =~ ^[0-9]+$ ]] || die "--mtu must be a number, got '$MTU'"
    (( MTU >= 1280 )) || die "--mtu must be at least 1280"
fi

ip link show "$LINK" &>/dev/null || die "parent interface '$LINK' does not exist"

# Refuse to build on the interface holding the default route. Latitude's
# private port is the *second* one; pointing this at the public port would
# not obviously break anything at first, then cut the SSH session the
# moment an MTU change or teardown touched it.
DEFAULT_IF=$(ip route show default 2>/dev/null | awk '/^default/ {print $5; exit}')
if [[ -n "$DEFAULT_IF" && "$DEFAULT_IF" == "$LINK" ]]; then
    die "'$LINK' carries the default route — that is the public interface, not \
the private one. Latitude's private network is the second port (usually eno2)."
fi

# No carrier means the switch side is not live. Latitude has to attach the
# server to the VLAN before this can work, so say that rather than letting
# the operator hunt for a config typo.
if [[ "$(cat "/sys/class/net/${LINK}/carrier" 2>/dev/null || echo 0)" != "1" ]]; then
    die "'$LINK' has no carrier. Confirm the server is attached to the VLAN in \
the Latitude dashboard before configuring the OS side."
fi

MAX_MTU=$(cat "/sys/class/net/${LINK}/tx_queue_len" &>/dev/null && \
    ip -d link show "$LINK" | awk '/maxmtu/ {for (i=1;i<=NF;i++) if ($i=="maxmtu") print $(i+1)}' | head -1)
if [[ -n "$MTU" && -n "$MAX_MTU" ]] && (( MTU + 4 > MAX_MTU )); then
    die "--mtu $MTU needs a parent MTU of $((MTU + 4)) for the 802.1Q tag, but \
'$LINK' supports at most ${MAX_MTU}"
fi

echo "=== Private network setup ==="
echo "  Parent link:  ${LINK}"
echo "  VLAN tag:     ${VLAN_ID}"
echo "  Interface:    ${VLAN_IF}"
echo "  Address:      ${ADDRESS}"
[[ -n "$MTU" ]] && echo "  MTU:          ${MTU} (parent raised to $((MTU + 4)))"
echo ""

# -------------------------------------------------------------------------
# Live configuration, via `ip` so the management link is never re-applied.
# Idempotent: an existing interface is reconfigured in place rather than
# treated as an error, so a re-run converges instead of failing.
# -------------------------------------------------------------------------
if ip link show "$VLAN_IF" &>/dev/null; then
    echo "  ${VLAN_IF} already exists — reconciling"
else
    ip link add link "$LINK" name "$VLAN_IF" type vlan id "$VLAN_ID"
    echo "  created ${VLAN_IF}"
fi

# The resulting MTU depends only on the arguments, never on what a previous
# run left behind: omitting --mtu returns the link to the 1500 default rather
# than silently inheriting a jumbo setting from an earlier experiment.
# Applied only when it actually differs, because changing the MTU resets the
# adapter and drops the link for a second or two.
DESIRED_MTU="${MTU:-1500}"
DESIRED_PARENT_MTU=$((DESIRED_MTU + 4))
CURRENT_PARENT_MTU=$(cat "/sys/class/net/${LINK}/mtu" 2>/dev/null || echo 0)
CURRENT_VLAN_MTU=$(cat "/sys/class/net/${VLAN_IF}/mtu" 2>/dev/null || echo 0)

if [[ "$CURRENT_PARENT_MTU" != "$DESIRED_PARENT_MTU" || "$CURRENT_VLAN_MTU" != "$DESIRED_MTU" ]]; then
    ip link set dev "$LINK" mtu "$DESIRED_PARENT_MTU"
    ip link set dev "$VLAN_IF" mtu "$DESIRED_MTU"
    echo "  MTU → ${DESIRED_MTU} (parent ${DESIRED_PARENT_MTU}); the NIC resets on this."
    echo "  Waiting for the link — 10GBASE-T retraining can take 15-30s."
    if wait_for_carrier "$LINK" 60; then
        echo "  ${LINK} carrier restored"
    else
        die "'$LINK' did not regain carrier within 60s of the MTU change. The \
link usually comes back on its own — check 'ethtool $LINK' before assuming \
the VLAN is misconfigured."
    fi
else
    echo "  MTU already ${DESIRED_MTU}"
fi

# Drop any address we did not ask for, so a re-run with a changed address
# leaves exactly one and the host cannot answer on a stale one.
while read -r stale; do
    [[ -z "$stale" ]] && continue
    if [[ "$stale" != "$ADDRESS" ]]; then
        ip addr del "$stale" dev "$VLAN_IF"
        echo "  removed stale address ${stale}"
    fi
done < <(ip -o -4 addr show dev "$VLAN_IF" 2>/dev/null | awk '{print $4}')

if ip -o -4 addr show dev "$VLAN_IF" | awk '{print $4}' | grep -qx "$ADDRESS"; then
    echo "  address ${ADDRESS} already present"
else
    ip addr add "$ADDRESS" dev "$VLAN_IF"
    echo "  added ${ADDRESS}"
fi

ip link set dev "$VLAN_IF" up
echo "  ${VLAN_IF} up"

# -------------------------------------------------------------------------
# Persistence. A separate netplan file, never an edit of 50-cloud-init.yaml:
# cloud-init owns that one and may rewrite it, silently dropping our VLAN on
# some future boot. Netplan merges *.yaml in lexical order, so 60- lands
# after cloud-init's 50- and the parent interface it declares.
# -------------------------------------------------------------------------
echo ""
echo "=== Persisting to ${NETPLAN_FILE} ==="
{
    echo "# Melin private network — generated by scripts/private-net-setup.sh."
    echo "# Do not edit: regenerated on every run. Do not move this into"
    echo "# 50-cloud-init.yaml either; cloud-init owns that file and may"
    echo "# rewrite it, taking the VLAN with it."
    echo "network:"
    echo "  version: 2"
    if [[ -n "$MTU" ]]; then
        # The parent needs headroom for the 4-byte tag. Repeat the match
        # cloud-init uses so merging cannot cost the interface its name.
        LINK_MAC=$(cat "/sys/class/net/${LINK}/address")
        echo "  ethernets:"
        echo "    ${LINK}:"
        echo "      match:"
        echo "        macaddress: ${LINK_MAC}"
        echo "      set-name: ${LINK}"
        echo "      mtu: ${DESIRED_PARENT_MTU}"
    fi
    echo "  vlans:"
    echo "    ${VLAN_IF}:"
    echo "      id: ${VLAN_ID}"
    echo "      link: ${LINK}"
    echo "      addresses: [${ADDRESS}]"
    [[ -n "$MTU" ]] && echo "      mtu: ${DESIRED_MTU}"
} > "$NETPLAN_FILE"
# netplan refuses to read world-readable configs on recent releases.
chmod 600 "$NETPLAN_FILE"
echo "  written (0600)"

# Validate the file now rather than discovering at the next boot that the
# host comes up without its private network.
if netplan generate 2>&1; then
    echo "  netplan generate: OK — config is valid for the next boot"
else
    die "netplan rejected ${NETPLAN_FILE}. The live interface is up, but this \
host would lose its private network on reboot."
fi

# -------------------------------------------------------------------------
# Verification.
# -------------------------------------------------------------------------
echo ""
echo "=== Verifying ==="
FAILED=0

if ip link show "$VLAN_IF" up &>/dev/null; then
    echo "  ok: ${VLAN_IF} is up"
else
    echo "  FAIL: ${VLAN_IF} is not up" >&2
    FAILED=1
fi

if ip -o -4 addr show dev "$VLAN_IF" | awk '{print $4}' | grep -qx "$ADDRESS"; then
    echo "  ok: ${ADDRESS} is assigned"
else
    echo "  FAIL: ${ADDRESS} is not assigned" >&2
    FAILED=1
fi

ACTUAL_ID=$(ip -d link show "$VLAN_IF" | awk '/802.1Q/ {for (i=1;i<=NF;i++) if ($i=="id") print $(i+1)}' | head -1)
if [[ "$ACTUAL_ID" == "$VLAN_ID" ]]; then
    echo "  ok: tagging frames with VLAN ${VLAN_ID}"
else
    echo "  FAIL: interface reports VLAN id '${ACTUAL_ID}', expected ${VLAN_ID}" >&2
    FAILED=1
fi

if [[ -n "$PEER" ]]; then
    # The only check that proves the switch side is really carrying our tag.
    # Establish basic reachability first, so a jumbo failure below can be
    # reported as "big frames blocked" rather than "peer is dead" — the two
    # have completely different causes.
    if ping_peer "$VLAN_IF" "$PEER"; then
        RTT=$(ping -c 10 -i 0.2 -W 2 -I "$VLAN_IF" "$PEER" 2>/dev/null | tail -1)
        echo "  ok: ${PEER} reachable over ${VLAN_IF}"
        echo "      ${RTT}"

        if [[ -n "$MTU" ]]; then
            # An unsupported MTU does not error, it silently blackholes large
            # frames. Probe at full size with DF set to find out now rather
            # than during a bench. Small frames already work at this point,
            # so a failure here isolates cleanly to frame size.
            if ping_peer "$VLAN_IF" "$PEER" "-M do -s $((MTU - 28))"; then
                echo "  ok: ${MTU}-byte frames traverse the VLAN"
            else
                echo "  FAIL: small frames pass but ${MTU}-byte frames do not, so the" >&2
                echo "        switch is not passing jumbo on this VLAN. Re-run without" >&2
                echo "        --mtu; the link works fine at the default 1500." >&2
                FAILED=1
            fi
        fi
    else
        echo "  FAIL: ${PEER} is not reachable over ${VLAN_IF}." >&2
        echo "        If the other host is not configured yet this is expected —" >&2
        echo "        run this script there too, then re-check. If both are" >&2
        echo "        configured, the VLAN tag or the Latitude attachment is wrong." >&2
        FAILED=1
        if [[ -n "$MTU" ]]; then
            echo "        (jumbo not probed — basic reachability has to work first)" >&2
        fi
    fi
else
    echo "  note: no --peer given, so only the local half is verified."
    echo "        Re-run with --peer <other-host-vlan-ip> to prove the link."
fi

echo ""
if [[ "$FAILED" -ne 0 ]]; then
    die "private network is not fully working — see the failures above"
fi
echo "=== Private network ready: ${VLAN_IF} ${ADDRESS} ==="
