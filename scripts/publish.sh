#!/usr/bin/env bash
set -euo pipefail

# Publish all workspace crates to crates.io in dependency order.
#
# Usage:
#   scripts/publish.sh           # dry-run (--dry-run passed to cargo publish)
#   scripts/publish.sh --execute # actually publish

DRY_RUN="--dry-run --allow-dirty"
if [[ "${1:-}" == "--execute" ]]; then
    DRY_RUN=""
    echo "==> LIVE publish mode"
else
    echo "==> Dry-run mode (pass --execute to publish for real)"
fi

# Topological order: leaves first, dependents last. The sequencer crates
# (melin-app, melin-pipeline, melin-journal, melin-wire-protocol,
# melin-transport-core, melin-server-runtime, melin-dpdk) are published
# from the Melin sequencer repository, not from here.
CRATES=(
    # Level 0: no internal dependencies
    melin-types
    melin-gateway-core

    # Level 1
    melin-protocol       # depends on: types
    melin-trading        # depends on: types

    # Level 2
    melin-exchange-core  # depends on: trading, types
    melin-market-data    # depends on: types, protocol
    melin-client         # depends on: protocol

    # Level 3
    melin-tui-fix-client # depends on: gateway-core

    # Level 4
    melin-server         # depends on: exchange-core, market-data, ...
    melin-oe-gateway     # depends on: gateway-core, protocol, exchange-core, types
    melin-md-gateway     # depends on: gateway-core, market-data, protocol, types
    melin-admin          # depends on: client, protocol
    melin-tui            # depends on: client, protocol

    # Level 5
    melin-bench          # depends on: server, ...
)

# Guard: make sure every publishable workspace member is in the list above
# (crates marked `publish = false` are exempt).
WORKSPACE_MEMBERS=$(cargo metadata --no-deps --format-version 1 \
    | python3 -c "import sys,json; print('\n'.join(sorted(p['name'] for p in json.load(sys.stdin)['packages'] if p.get('publish') != [])))")
SCRIPT_MEMBERS=$(printf '%s\n' "${CRATES[@]}" | sort)

MISSING=$(comm -23 <(echo "$WORKSPACE_MEMBERS") <(echo "$SCRIPT_MEMBERS"))
if [[ -n "$MISSING" ]]; then
    echo "ERROR: workspace crates missing from publish list:"
    echo "$MISSING"
    echo "Add them to the CRATES array in scripts/publish.sh"
    exit 1
fi

EXTRA=$(comm -13 <(echo "$WORKSPACE_MEMBERS") <(echo "$SCRIPT_MEMBERS"))
if [[ -n "$EXTRA" ]]; then
    echo "ERROR: crates in publish list but not in workspace:"
    echo "$EXTRA"
    exit 1
fi

DELAY=30

for crate in "${CRATES[@]}"; do
    echo "--- Publishing $crate ---"
    OUTPUT=$(cargo publish -p "$crate" $DRY_RUN 2>&1) && STATUS=0 || STATUS=$?
    if [[ $STATUS -ne 0 ]]; then
        if echo "$OUTPUT" | grep -q "already exists"; then
            echo "    Already published, skipping."
            continue
        fi
        echo "$OUTPUT" >&2
        exit $STATUS
    fi
    echo "$OUTPUT"
    if [[ -z "$DRY_RUN" ]]; then
        echo "    Waiting ${DELAY}s for crates.io to index..."
        sleep "$DELAY"
    fi
done

echo "==> Done."
