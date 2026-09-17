# Changelog

All notable changes to Melin Exchange Core are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Every published crate in the workspace shares a single version number, so an
entry here covers all of them.

While the project is at `0.x`, a minor release may contain breaking changes.
Anything that breaks a deployment or a Rust dependent is called out under
**Changed** or **Removed**. Releases before 0.16.0 predate this file; their
tags carry the release notes.

The node runtime — journaling, replication, transport, `--cores` and the other
server flags it owns — comes from the Melin sequencer. Its own changelog has the
full detail behind entries marked *(sequencer)*.

## [Unreleased]

## [0.16.0] - 2026-09-14

### Changed

- **Every binary is renamed with the `melin-ec-` prefix**: `melin-ec-server`,
  `melin-ec-admin`, `melin-ec-keygen`, `melin-ec-promote`, `melin-ec-tui`,
  `melin-ec-tui-fix-client`, `melin-ec-oe-gateway`, `melin-ec-md-gateway`,
  `melin-ec-bench`, `melin-ec-plot` and `melin-ec-replication-bench`. Update
  service units, deployment scripts and anything that matches process names.
  The Docker image ships the new names.
- **Log targets follow the rename**: `RUST_LOG=melin_server=debug` is now
  `RUST_LOG=melin_ec_server=debug`, and the matching engine's target is
  `melin_ec`.
- **Every crate is renamed with the `melin-ec-` prefix**, so exchange crates can
  no longer collide with the sequencer's on crates.io. The matching engine
  `melin-exchange-core` is `melin-ec`; `melin-types`, `melin-protocol`,
  `melin-trading`, `melin-market-data`, `melin-gateway-core`, `melin-client`,
  `melin-server` and the binary crates take the prefix, and library paths
  follow (`melin_ec::`, `melin_ec_protocol::`, `melin_ec_client::`, …). The
  crates.io name `melin-client` now belongs to the sequencer: its 0.15.0 is the
  old exchange client, and 0.16.0 onward is a different crate. Depend on
  `melin-ec-client` instead.
- **`--cores` takes named entries** *(sequencer)*, e.g.
  `--cores journal-seq=1,matching=2,response=3,reader=4,...`, in any order.
  `journal-disk` and `journal-prep` are required. Each entry may carry a wait
  policy: a bare core (or `7s`) busy-spins and owns the core, `7y` spins
  briefly then yields and may share it. A node refuses to start when a
  busy-spinning thread shares its core, and warns at boot when a mandatory
  thread has no core. The benches' `--cores` and `--pipeline-cores` use the
  same syntax. See the operations guide.
- **The journal's sequencing thread is named `journal-seq`** *(sequencer)*,
  which is what process listings and CPU-pinning audits now show.
- **Replicas serve the health endpoint** *(sequencer)*, with or without
  election. A primary and a replica on one host need distinct `--health-bind`
  addresses.
- **`--dpdk-eal-args` requires the joined form** *(sequencer)*
  (`--dpdk-eal-args="-l 1 ..."`).
- **The sequencer is upgraded to 0.16.** Journal, snapshot and replication
  formats are unchanged, and so is the client wire format.

### Added

- **`--journal-staging-mode <zero-fill|allocate>`** *(sequencer)* chooses how
  the next journal segment is staged. `zero-fill` stays the default; consider
  `allocate` on network-attached volumes. A warning now fires when the journal
  extends past its pre-written region.
- **Signing keys may be PKCS#8 PEM** as written by
  `openssl genpkey -algorithm ed25519`, as well as the 32-byte raw seed,
  everywhere a key file is read: the admin tool, the TUI, the gateways, the
  bench and market-data.

### Removed

- **`--yield-idle`** *(sequencer)*. Suffix every pinned `--cores` entry with
  `y` instead.
- **The transport's frames are gone from `melin-ec-protocol`**: handshake,
  heartbeat, batch-end, busy and engine-error variants. Readers classify those
  through the sequencer's `melin-client`, and only application responses reach
  the exchange codec. Source-breaking for Rust dependents only.

### Fixed

- The exchange client reports an engine error as `ClientError::EngineError`
  once its batch has ended and keeps the connection usable, so the admin tool
  and TUI keep serving after one.
- A FIX order-entry session whose node reports busy during request-sequence
  sync stays parked instead of closing.
- An unpinned server thread runs on the process's own CPU set *(sequencer)*
  instead of inheriting an isolated core from the thread that started it.

[Unreleased]: https://github.com/melin-engine/exchange-core/compare/v0.16.0...HEAD
[0.16.0]: https://github.com/melin-engine/exchange-core/releases/tag/v0.16.0
