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

## [0.17.0] - 2026-09-22

This release adopts Melin 0.17.0. The [0.17.0 section of its
changelog](https://github.com/melin-engine/melin/blob/main/CHANGELOG.md#0170---2026-09-22)
lists further fixes in the node runtime that apply here as they are; the
entries below cover what changes for this product. One of them needs an
operator action: a snapshot written by an earlier release may hold the
effect of a request that was refused as a duplicate. The snapshot format
bump below already has every node refuse such a snapshot, so the upgrade
starts by moving the snapshot aside and rebuilding from the journal, which
repairs this — a replica that was bootstrapped by snapshot transfer, and so
has no journal from sequence 1, is re-bootstrapped from a node that has.

### Changed

- **A halted node refuses a write before journaling it** *(sequencer)*. A
  primary that has lost its last replica rejects state-mutating requests with
  `ReplicaDisconnected`, as before, but the request is now turned away as it is
  read, so it never reaches the journal. Previously the rejection was decided
  after the journal had recorded the write, and a later replay — on restart, on
  a promoted replica, or on a replica catching up — applied what the client was
  told had failed. A refused request also consumes nothing, so a client may
  resend it under the same request sequence once trading resumes.
- **A superseded node closes client connections instead of rejecting**
  *(sequencer)*. A node fenced by a newer primary is stopping; clients
  reconnect and land on the new primary, as they would after a crash.
- **Documented that a halted node answers no query** *(sequencer)*. This is
  long-standing behavior, not a change in this release: `QueryStats`, position
  and request-sequence queries get no reply until the halt clears, under every
  ack policy, and connecting a client to a halted node blocks for the same
  reason. Monitor a halted node through the health endpoint. See "Halt on
  Replica Disconnect" in `docs/operations.md`.
- **The per-account limits are journaled.** `--max-orders-per-account`,
  `--max-orders-per-second` and `--max-orders-burst` used to be applied by
  every node from its own flags, so nodes started with different values
  enforced different limits and could diverge. Now a node records its values
  in the journal each time it becomes primary, and every node enforces the
  recorded ones: a replica follows the primary's limits, and its own flags
  take effect only once it is promoted. Changing a limit takes a primary
  restart or a failover. See "Per-account limits" in `docs/operations.md`.
- **A node starts with no instrument and no account.** The genesis seed —
  `--instruments` placeholder instruments and `--accounts` accounts funded
  out of nothing — was a development fixture that every build emitted, so
  a production node minted balances on its first start and kept them in
  the journal forever. It is now behind the `synthetic-seed` build
  feature, off by default. Register instruments and provision accounts
  through the admin client instead. `--accounts` and `--instruments` keep
  sizing the node either way. Benches, smoke tests and the demo scripts
  build with the feature; a node that does logs a warning when it seeds.
  See "Starting empty" in `docs/operations.md`.
- **Snapshot format v19** carries the per-account limits. A node refuses to
  start from a snapshot written by an earlier release.
- **Replicas pre-allocate their memory before streaming** *(sequencer)*.
  `--accounts` and `--instruments` now size a replica too, before it applies
  its first event, instead of the replica growing its collections on the
  primary's acknowledgement path. Expect a replica's resident memory to match
  its primary's from startup.
- **Every engine a node builds is production-sized from the start.** A node
  used to size its collections after replaying its journal, which left a
  restarted node running on whatever the replay had grown and paying the
  growth on the matching thread. The engine is now built at production
  capacity before the first event, and a snapshot restore rebuilds and
  pre-faults it the same way — including the shadow copy that writes
  snapshots. Expect roughly 150 MB more resident memory per node for that
  copy, and about a tenth of a second more startup per restore.
  `--accounts` and `--instruments` size the balance map on every node and
  every start, a journal replay included *(sequencer)*, and `--accounts`
  now also sizes the per-account maps for a deployment past a million
  accounts. A node restored from a snapshot rebuilds its balance map to
  those counts at startup, which takes a few seconds and twice the map's
  memory while it runs.
- **Rust dependents:** `AppFactory` is gone *(sequencer)*. The server is
  started with `StartupConfig` (which builds the startup events and the
  sizing). `ServerApp` implements `Default` as an empty, production-sized,
  pre-faulted engine, and its `restore` produces the same shape; its
  `prefault` reserves only what `--accounts` and `--instruments` size.
  `Exchange::prefault_seed` is gone: `with_capacity` and a snapshot restore
  reserve production capacity themselves, and
  `Exchange::reserve_for_accounts` sizes the balance map and the
  per-account maps from the counts. `ServerApp::new` is
  gone: wrap an `Exchange` for a small one. `TradingEvent` gains
  `SetAccountLimits`. `StartupConfig::startup_events` seeds only under
  `synthetic-seed`; a dependent that needs the seed regardless calls
  `synthetic_startup_events`.

### Fixed

- **Resting orders are no longer lost when a primary restarts or a replica is
  promoted.** Pre-allocating memory at startup cleared the order books'
  lookup indexes. It ran after the book had been rebuilt from the journal, so
  every order resting at the time of a restart or failover stayed on the book
  but could no longer be cancelled or amended. A cancel got an empty reply,
  and the order id stayed blocked (`DuplicateOrderId`). Affects 0.15.0 and
  0.16.0.

### Added

- **`melin_writes_refused_total` on `/metrics`** *(sequencer)*, counting client
  writes turned away while the node was halted. A refused write is never
  journaled, so this counter is its only trace on the node.

### Removed

- **The `Superseded` reject reason**, and wire code 21 with it. Nothing produces
  it any more (see above), so the code is now rejected as invalid on decode and
  stays reserved — clients that map it should drop that arm. Rust dependents
  matching on `RejectReason` drop the variant.

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

[Unreleased]: https://github.com/melin-engine/exchange-core/compare/v0.17.0...HEAD
[0.17.0]: https://github.com/melin-engine/exchange-core/releases/tag/v0.17.0
[0.16.0]: https://github.com/melin-engine/exchange-core/releases/tag/v0.16.0
