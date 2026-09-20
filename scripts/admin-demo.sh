#!/usr/bin/env bash
# Quick-start script: builds everything, generates a key, starts the server,
# and launches the admin TUI. Cleans up on exit.
set -euo pipefail

TMPDIR=$(mktemp -d)
trap 'kill $SERVER_PID 2>/dev/null; rm -rf "$TMPDIR"' EXIT

echo "==> Building..."
# `synthetic-seed` so the TUI opens on an exchange with instruments and
# funded accounts in it; a stock server starts empty.
cargo build -p melin-ec-server --bin melin-ec-server --features synthetic-seed --quiet
cargo build --bin melin-ec-keygen --bin melin-ec-admin --quiet

echo "==> Generating keypair..."
cd "$TMPDIR"
cargo run --manifest-path "$OLDPWD/Cargo.toml" --bin melin-ec-keygen --quiet -- admin operator
# Extract the authorized_keys line from keygen output and write to file.
echo "operator $(cat admin.pub | tr -d '\n') admin" > authorized_keys

echo "==> Starting server..."
cargo run --manifest-path "$OLDPWD/Cargo.toml" -p melin-ec-server --bin melin-ec-server \
    --features synthetic-seed --quiet -- \
    --authorized-keys "$TMPDIR/authorized_keys" \
    --journal "$TMPDIR/demo.journal" &
SERVER_PID=$!
sleep 0.5

echo "==> Launching admin TUI (Esc to quit)..."
cargo run --manifest-path "$OLDPWD/Cargo.toml" --bin melin-ec-admin --quiet -- \
    127.0.0.1:9876 "$TMPDIR/admin.key"
