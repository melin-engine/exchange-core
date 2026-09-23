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

This release adopts the sequencer's next release. The [Unreleased section of
its changelog](https://github.com/melin-engine/melin/blob/main/CHANGELOG.md#unreleased)
lists further fixes in the node runtime that apply here as they are; the
entries below cover what changes for this product. One entry there does not
apply: `--max-orders-per-account`, `--max-orders-per-second` and
`--max-orders-burst` leave the sequencer's flags, but the exchange node
declares them itself, with the same names, defaults and behavior.

This release carries no state over from 0.17.0. The journal format changes
(see below) and a node refuses a journal written by an earlier release, so a
node upgrades onto a fresh journal and is re-provisioned through the admin
client. The snapshot repair that 0.17.0 asked for is moot on this path: a
snapshot from an earlier release is never read.

### Changed

- **Duplicate requests are refused by the engine, on every path.** The
  node runtime no longer keeps the per-key request-sequence gate
  *(sequencer)*; the sequence a client stamps on each request now travels
  inside the journaled event, and the engine refuses a repeat itself,
  before applying anything. Clients see the same `DuplicateRequest`
  rejection as before, from the same request sequence in the same frame.
  What changes is where the verdict is reached: in the engine, on every
  path an event takes — live, on a journal replay, on a replica, and in
  the copy of the engine that snapshots are written from. That copy used
  to apply what the primary had refused, so a retried request could land
  twice in a snapshot; it cannot any more. One consequence: a refused
  duplicate is still an event the node clocks, so scheduled work due at
  its timestamp — an order expiry, say — fires on it as on any other
  event, where it used to wait for the next accepted one.
- **Journal format 15** *(sequencer)*. Entries no longer carry a
  runtime-level request sequence, which now lives in the event. A node
  refuses a format-14 journal.
- **Replication protocol 5** *(sequencer)*. A primary and a replica on
  different releases refuse each other at the handshake; upgrade a cluster
  together.
- **The authentication handshake frame loses its sequence prefix**
  *(sequencer)*. A `ChallengeResponse` is now `[tag][signature][public
  key]`. Every client shipped here authenticates through the sequencer's
  client and follows; a client built against an earlier release fails the
  handshake and has its key refused.
- **Exchange messages travel under the node's frame tag.** Every frame is
  now `[length][tag][body]`, and the tag is the node's alone
  *(sequencer)*: requests and exchange responses travel under tag `0x09`,
  and every other tag is one of the node's own frames — the handshake,
  heartbeats, `BatchEnd`, `ServerBusy`, `EngineError`. The message kind
  that used to be the tag opens the body instead, with unchanged values.
  A request is now `[length][0x09][kind][seq][payload]` (it was
  `[length][seq][kind][payload]`) and an exchange response
  `[length][0x09][kind][payload]` (it was `[length][kind][payload]`).
  Every client shipped here follows. A client built against an earlier
  release must not be pointed at this one. Most of its requests are
  dropped or refused as malformed: the node logs each at `debug!` and
  keeps the connection, so the client sees only its own read timeouts.
  But the node reads the low byte of its `seq` as the tag, so a request
  whose `seq` ends in `0x09` is taken as an exchange message and can
  decode as a different request, with a sequence that locks the key out
  of later ones. The client reads every exchange response as a protocol
  error.
- **The node reads and writes the protocol's framing** *(sequencer)*. A
  client frame under any tag but `0x09` after the handshake is dropped at
  `debug!` before the exchange sees it, like any other malformed request.
  The node writes the length and the tag around every response body the
  exchange encodes.
- **A query cannot change engine state** *(sequencer)*. The stats,
  position and request-sequence queries are answered from a read-only
  view of the engine, and no longer advance its clock, so a query cannot
  fire scheduled work — an order expiry, say — that a journal replay or
  a replica would then not fire. The counters a stats query reports are
  the node's own, read when the query is answered.
- **Some node log lines and metric descriptions are reworded**
  *(sequencer)*. No metric, flag or health-endpoint field is renamed, but
  an alert that matches on message text needs updating: `all replicas
  disconnected — trading halted` is now `all replicas disconnected —
  halted, refusing client writes`, and `raft core stopped — control plane
  down, trading unaffected` now ends `sequencing unaffected`.
- **A replication key can no longer connect as a client.** It authorizes
  streaming between nodes and nothing else. The trading port refuses it
  during the handshake *(sequencer)*, and so does the event feed
  (`--event-bind`), which used to admit any listed key and stream every
  execution to it. A client or subscriber that used a replication key
  needs a key of its own, under a client role.
- **An `authorized_keys` file that lists a key twice no longer loads**
  *(sequencer)*. The last line used to win silently. The node refuses to
  start and names the line, so remove the duplicate before upgrading.
- **A snapshot whose exchange state does not end where the file does is
  refused.** Extra bytes mean it was written to a layout this release does
  not know, so the state it would restore is not the one saved. A node
  refuses to start from it, as from any corrupt snapshot, and the shadow
  stage's copy of the engine fails the same way *(and sequencer)*.
- **Rust dependents:** `RequestDecoder::decode` takes `(body,
  permission)` and `ResponseEncoder` returns the length of the body it
  wrote, both as the sequencer now asks *(sequencer)*. The codec's
  unsuffixed functions work on whole frames: `encode_request` and
  `encode_response` write the length and the tag, and `decode_request`
  and `decode_response` take a frame after its length prefix and refuse
  one under any tag but `0x09`. Their `_body` forms work on the body
  alone: `encode_request_body`, `decode_request_body`,
  `encode_response_body` and `decode_response_body`. A reply that
  `melin_client::classify` returns is a body, so decode it with
  `decode_response_body`. `codec::app_frame_body` strips the tag from a
  frame, and `codec::MAX_REQUEST_BODY` and `MAX_RESPONSE_BODY` are the
  widest body of each, checked against the node runtime's bounds at
  compile time.
  `ServerApp::apply` returns nothing and `ServerApp::query` answers the
  queries, from a `QueryCtx` that carries the node counters `ApplyCtx`
  used to *(sequencer)*.
- **Rust dependents:** the sequencer's event is now `TradingRequest`, a
  `TradingEvent` with its `request_seq`, and `TradingEvent` no longer
  implements `AppEvent` (its codec is inherent). Journal readers and
  writers, `InputSlot` and `StartupEvents` are typed on `TradingRequest`;
  an event the node journals itself is `TradingRequest::internal(event)`.
  `Application::check_request_seq` is gone *(sequencer)*: `ServerApp::apply`
  runs `Exchange::check_request_seq` itself, and `build_reject` no longer
  sees `DuplicateRequest`. `melin_journal`'s `encode`, `decode`,
  `batch_append_with_ts` and `JournalEntry` lose their `request_seq`
  *(sequencer)*.
- **Rust dependents:** `ServerConfig` no longer has `max_orders_per_account`,
  `max_orders_per_second` or `max_orders_burst` *(sequencer)*. They live on
  `StartupConfig` alone, which now derives `clap::Args` (flatten it into a
  command line) and `Default` (the flags' defaults). `melin_app::EncodeReport`
  is removed *(sequencer)*.

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
