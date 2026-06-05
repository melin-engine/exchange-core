#!/usr/bin/env bash
# Compare journal integrity across two servers using BLAKE3 chain hashes.
#
# Builds and runs the journal-verify binary on each server, then
# compares the output. Matching tail_chain + last sequence means the
# journals contain identical event streams (the tail chain hash
# commits to every entry in the lineage).
#
# Usage:
#   ./scripts/journal-verify.sh <server1> <journal1> <server2> <journal2>
#
# Example:
#   ./scripts/journal-verify.sh root@primary /mnt/journal/bench.journal \
#                                root@replica /mnt/journal/replica.journal

set -euo pipefail

if [[ $# -lt 4 ]]; then
    echo "usage: $0 <server1> <journal1> <server2> <journal2>"
    exit 1
fi

SERVER1="$1"
JOURNAL1="$2"
SERVER2="$3"
JOURNAL2="$4"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
REPO_DIR="~/workspace/melin"

echo "=== Journal Verification ==="
echo ""

# Build the verify tool on both servers (cached — instant if already built).
for HOST in "$SERVER1" "$SERVER2"; do
    ssh $SSH_OPTS "$HOST" "cd ${REPO_DIR} && source ~/.cargo/env && \
        cargo build --release -p melin-server --bin journal-verify 2>&1 | tail -1"
done

# Run the verifier on a host, echoing its full report even when the
# binary exits 1 (lineage FAILED) — a bare `VAR=$(ssh …)` under
# `set -e` would abort before printing the captured diagnosis.
run_verify() {
    local host="$1" journal="$2" out
    if ! out=$(ssh $SSH_OPTS "$host" "cd ${REPO_DIR} && ./target/release/journal-verify ${journal}"); then
        echo "$out" | sed 's/^/    /' >&2
        echo "  FAILED — lineage verification failed on ${host}" >&2
        exit 1
    fi
    echo "$out"
}

echo "  Server 1: ${SERVER1} → ${JOURNAL1}"
OUT1=$(run_verify "$SERVER1" "$JOURNAL1")
echo "$OUT1" | sed 's/^/    /'
echo ""

echo "  Server 2: ${SERVER2} → ${JOURNAL2}"
OUT2=$(run_verify "$SERVER2" "$JOURNAL2")
echo "$OUT2" | sed 's/^/    /'
echo ""

# Extract tail chain hashes and last sequences from the lineage
# report (`tail_chain:    <hex>` and `range:    <first>..=<last>`).
# awk exits 0 on no match, so a format drift surfaces as the empty-
# variable check below instead of a silent set -e death.
HASH1=$(echo "$OUT1" | awk '/tail_chain:/ {print $2}')
HASH2=$(echo "$OUT2" | awk '/tail_chain:/ {print $2}')
SEQ1=$(echo "$OUT1" | awk '/range:/ {sub(/.*\.\.=/, "", $2); print $2}')
SEQ2=$(echo "$OUT2" | awk '/range:/ {sub(/.*\.\.=/, "", $2); print $2}')

if [[ -z "$HASH1" || -z "$HASH2" || -z "$SEQ1" || -z "$SEQ2" ]]; then
    echo "  ERROR — could not parse tail_chain/range from journal-verify output" >&2
    echo "    (did the binary's report format change?)" >&2
    exit 1
fi

# A build without the hash-chain feature prints
# `tail_chain:    (hash-chain disabled in this build)` — fall back to
# comparing sequences only rather than vacuously matching the notice.
if [[ "$HASH1" == "("* || "$HASH2" == "("* ]]; then
    if [[ "$SEQ1" == "$SEQ2" ]]; then
        echo "  MATCH (sequences only — hash chain disabled in at least one build; last_seq=${SEQ1})"
    else
        echo "  MISMATCH — last sequences differ!"
        echo "    seq1=${SEQ1} seq2=${SEQ2}"
        exit 1
    fi
elif [[ "$HASH1" == "$HASH2" && "$SEQ1" == "$SEQ2" ]]; then
    echo "  MATCH — journals are consistent (last_seq=${SEQ1}, tail_chain=${HASH1})"
else
    echo "  MISMATCH — journals differ!"
    echo "    tail_chain1=${HASH1} last_seq1=${SEQ1}"
    echo "    tail_chain2=${HASH2} last_seq2=${SEQ2}"
    exit 1
fi
