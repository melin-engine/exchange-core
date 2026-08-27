#!/usr/bin/env bash
# Tests for private-net-setup.sh's argument validation and help output.
#
# The script itself reconfigures a NIC, so what can be tested off-hardware
# is the half that decides whether to touch the network at all: the
# argument validators, and the fact that they run before the root check so
# a mistyped VLAN id is named whether or not the caller remembered sudo.
#
# The function bodies are extracted from the script and sourced, so these
# tests exercise the shipping code rather than a copy of it.
#
# Every invocation of the real script below passes deliberately invalid
# arguments. That is not incidental: it guarantees the script exits during
# validation and never reaches the code that would configure an interface,
# so this file is safe to run as root.
#
# Usage: ./scripts/test-private-net-setup.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_SCRIPT="${SCRIPT_DIR}/private-net-setup.sh"

if [[ ! -f "$SETUP_SCRIPT" ]]; then
    echo "error: cannot find private-net-setup.sh at $SETUP_SCRIPT" >&2
    exit 1
fi

TMP_ROOT=$(mktemp -d)
trap 'rm -rf "$TMP_ROOT"' EXIT

# Anchoring on `^name() {` .. `^}` works because the file is shfmt-shaped:
# the closing brace of a top-level function is the only `}` in column 0
# within its range.
extract_fn() {
    awk "/^$1\(\) \{/,/^\}/" "$SETUP_SCRIPT"
}

FN_FILE="${TMP_ROOT}/fns.sh"
{
    extract_fn validate_vlan_id
    extract_fn validate_address
    extract_fn validate_mtu
    extract_fn parent_mtu_for
} > "$FN_FILE"

for fn in validate_vlan_id validate_address validate_mtu parent_mtu_for; do
    if ! grep -q "^${fn}() {" "$FN_FILE"; then
        echo "error: failed to extract ${fn} from private-net-setup.sh — has it" >&2
        echo "  been renamed or reformatted? These tests are now vacuous; fix" >&2
        echo "  the extraction before trusting a pass." >&2
        exit 1
    fi
done

# shellcheck source=/dev/null
source "$FN_FILE"

PASS=0
FAIL=0

ok() {
    PASS=$((PASS + 1))
    echo "  ok — $1"
}

bad() {
    FAIL=$((FAIL + 1))
    echo "  FAIL — $1" >&2
    [[ -n "${2:-}" ]] && echo "         $2" >&2
}

# Assert a validator accepts every value given.
accepts() {
    local fn="$1"; shift
    local v
    for v in "$@"; do
        if ! "$fn" "$v" 2>/dev/null; then
            bad "${fn} should accept '${v}'"
            return
        fi
    done
    ok "${fn} accepts: $*"
}

# Assert a validator rejects every value given, and says something.
rejects() {
    local fn="$1" label="$2"; shift 2
    local v out
    for v in "$@"; do
        if out=$("$fn" "$v" 2>&1); then
            bad "${fn} should reject '${v}'" "returned 0"
            return
        fi
        if [[ -z "$out" ]]; then
            bad "${fn} rejected '${v}' silently" "a refusal with no reason is not actionable"
            return
        fi
    done
    ok "${fn} rejects ${label}"
}

echo "=== private-net-setup.sh argument validation ==="

# ---------------------------------------------------------------------
# VLAN id. The dashboard shows both a numeric 802.1Q tag and a `vlan_xxx`
# resource id, so the non-numeric case is the one operators actually hit.
# ---------------------------------------------------------------------
accepts validate_vlan_id 1 2025 4094
rejects validate_vlan_id "non-numeric, out-of-range and empty tags" \
    "" "abc" "vlan_5f3a" "0" "4095" "20.25" "-1"

if out=$(validate_vlan_id "vlan_5f3a" 2>&1); then
    bad "a resource id must not be accepted as a tag"
elif grep -q "vlan_xxx" <<< "$out"; then
    ok "the resource-id confusion is named in the error"
else
    bad "the error should point at the vlan_xxx/tag confusion" "$out"
fi

# ---------------------------------------------------------------------
# Address. A bare */* test let `foo/bar` through to `ip addr add`, which
# fails with a raw iproute2 error instead of a message like these.
# ---------------------------------------------------------------------
accepts validate_address "10.8.0.1/24" "0.0.0.0/0" "192.168.1.255/32"
rejects validate_address "malformed CIDRs" \
    "" "10.8.0.1" "foo/bar" "10.8.0.999/24" "10.8.0.1/33" "10.8.0/24" "10.8.0.1/" "/24"

# ---------------------------------------------------------------------
# MTU, and the 4-byte tag headroom the parent needs.
# ---------------------------------------------------------------------
accepts validate_mtu 1280 1500 9000
rejects validate_mtu "non-numeric and sub-1280 values" "" "abc" "1279" "0" "-1" "1500.5"

if [[ "$(parent_mtu_for 1500)" == "1504" && "$(parent_mtu_for 9000)" == "9004" ]]; then
    ok "parent_mtu_for adds the 4-byte 802.1Q tag"
else
    bad "parent MTU must be the VLAN MTU plus 4" \
        "1500 -> $(parent_mtu_for 1500), 9000 -> $(parent_mtu_for 9000)"
fi

echo ""
echo "=== private-net-setup.sh help output ==="

# ---------------------------------------------------------------------
# `usage` prints the header comment. It used to be a fixed `2,40p` line
# range, which had already drifted two lines past the comment block and
# was printing `set -euo pipefail` as though it were documentation.
# ---------------------------------------------------------------------
help_out=$(bash "$SETUP_SCRIPT" --help 2>&1)
help_rc=$?

if [[ "$help_rc" -eq 0 ]]; then
    ok "--help exits 0"
else
    bad "--help should exit 0" "rc=${help_rc}"
fi

if grep -q "Usage:" <<< "$help_out" && grep -q -- "--vlan-id" <<< "$help_out"; then
    ok "--help documents the options"
else
    bad "--help should print the usage block" "$help_out"
fi

# The specific regression: no line of the script's own source may appear.
if grep -qE '^\s*(set -|LINK=|VLAN_ID=|NETPLAN_DIR=)' <<< "$help_out"; then
    bad "--help is leaking source lines past the comment block" \
        "$(grep -nE '^\s*(set -|LINK=|VLAN_ID=|NETPLAN_DIR=)' <<< "$help_out" | head -2)"
else
    ok "--help stops at the end of the comment block"
fi

if [[ -n "$help_out" ]]; then
    ok "--help is not empty"
else
    bad "--help printed nothing"
fi

# Over-reading is the other half of the same bug and the leak check above
# misses it: a `usage` that never stops still prints only comment lines, so
# it trips no source-line pattern while dumping every comment in the file.
# Count the leading comment block here and require an exact match — that
# catches reading too far and stopping too early, including the original
# `2,40p` range which ran two lines long.
expected_help_lines=$(awk 'NR == 1 { next } /^#/ { n++; next } { exit } END { print n + 0 }' "$SETUP_SCRIPT")
actual_help_lines=$(printf '%s\n' "$help_out" | wc -l)
if [[ "$actual_help_lines" -eq "$expected_help_lines" ]]; then
    ok "--help prints exactly the leading comment block (${expected_help_lines} lines)"
else
    bad "--help length does not match the header comment block" \
        "expected ${expected_help_lines} lines, got ${actual_help_lines}"
fi

echo ""
echo "=== private-net-setup.sh validates before requiring root ==="

# ---------------------------------------------------------------------
# Argument mistakes must be named whether or not the caller used sudo.
# The script once checked --vlan-id before the root check but --address
# and --mtu after it, so half the arguments only reported their problem
# to a caller who had already elevated.
#
# Every case here is invalid by construction, so the script exits during
# validation and never touches an interface.
# ---------------------------------------------------------------------
expect_arg_error() {
    local label="$1" expect="$2"; shift 2
    local out rc
    out=$(bash "$SETUP_SCRIPT" "$@" 2>&1)
    rc=$?
    if [[ "$rc" -eq 0 ]]; then
        bad "${label}: should have failed" "$out"
    elif grep -q "must run as root" <<< "$out"; then
        bad "${label}: root check ran before the argument was validated" "$out"
    elif grep -q -- "$expect" <<< "$out"; then
        ok "${label}"
    else
        bad "${label}: unexpected message" "$out"
    fi
}

expect_arg_error "a non-numeric --vlan-id is named" "must be a number" \
    --vlan-id abc
expect_arg_error "an out-of-range --vlan-id is named" "must be 1-4094" \
    --vlan-id 4095
expect_arg_error "a missing --address is named" "--address is required" \
    --vlan-id 2025
expect_arg_error "a prefix-less --address is named" "must include a prefix length" \
    --vlan-id 2025 --address 10.8.0.1
expect_arg_error "a malformed --address is named" "dotted-quad" \
    --vlan-id 2025 --address foo/bar
expect_arg_error "an out-of-range octet is named" "above 255" \
    --vlan-id 2025 --address 10.8.0.999/24
expect_arg_error "an out-of-range prefix is named" "prefix length must be 0-32" \
    --vlan-id 2025 --address 10.8.0.1/99
expect_arg_error "a too-small --mtu is named" "at least 1280" \
    --vlan-id 2025 --address 10.8.0.1/24 --mtu 900
expect_arg_error "a non-numeric --mtu is named" "--mtu must be a number" \
    --vlan-id 2025 --address 10.8.0.1/24 --mtu jumbo
expect_arg_error "an unknown argument is named" "unknown argument" \
    --vlan-id 2025 --address 10.8.0.1/24 --frobnicate
expect_arg_error "--down still requires a tag" "--vlan-id is required" \
    --down

# --down needs no --address, so it must not be asked for one. Use an
# invalid tag so the run still stops in validation.
out=$(bash "$SETUP_SCRIPT" --down --vlan-id abc 2>&1)
if grep -q -- "--address" <<< "$out"; then
    bad "--down must not require --address" "$out"
else
    ok "--down does not demand an address"
fi

echo ""
echo "=== ${PASS} passed, ${FAIL} failed ==="
[[ "$FAIL" -eq 0 ]]
