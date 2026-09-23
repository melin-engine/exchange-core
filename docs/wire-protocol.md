# Wire Protocol Specification

Binary wire protocol for client-server communication over TCP or Unix domain sockets. Manual serialization (no serde) for zero allocation, predictable layout, and no format stability concerns across dependency versions.

All multi-byte integers are **little-endian**. No CRC on the wire -- TCP handles integrity. The protocol assumes a trusted network (isolated VLAN). It is NOT safe over untrusted networks without TLS or an equivalent transport-layer encryption.

## Frame Format

Every frame, in either direction, is a length prefix, a one-byte frame tag, and a body:

```
+----------------+----------+-----------+
| length (4B LE) | tag (1B) | body      |
+----------------+----------+-----------+
```

| Field   | Type | Size    | Description                                         |
|---------|------|---------|-----------------------------------------------------|
| length  | u32  | 4 bytes | Byte count of tag + body (excludes itself)          |
| tag     | u8   | 1 byte  | Frame tag: `0x09` for an exchange message, otherwise one of the node frames below |
| body    | ...  | 0..N    | For an exchange message, the request or response body below |

The frame tag belongs to the node. Tag `0x09` marks an exchange message: every request and every exchange response travels under it, and its body is laid out as described below. Every other tag is a node frame with a fixed meaning (see "Node Frames"). The node drops a client frame under any tag but `0x09` once the handshake is over, so a request framed any other way never reaches the exchange.

**Maximum frame size**: 1024 bytes (1 KiB) for the tag and body together, not counting the length prefix. A frame exceeding this limit is rejected and the connection is closed.

### Request Body

Every request body starts with its message kind and carries a per-key request sequence number for idempotency:

```
+-----------+-----------+-----------+
| kind (1B) | seq (8B)  | payload   |
+-----------+-----------+-----------+
```

| Field   | Type | Size    | Description                                              |
|---------|------|---------|----------------------------------------------------------|
| kind    | u8   | 1 byte  | Message type discriminant (see "Request Messages")       |
| seq     | u64  | 8 bytes | Per-key request sequence for idempotency (0 for heartbeat) |
| payload | ...  | 0..N    | Variant-specific fields                                  |

The `seq` field is a monotonically increasing counter per authentication key (see "Per-Key Idempotency"). Heartbeats and the read-only queries are exempt and may carry any `seq`; heartbeats use `0`.

### Response Body

Response bodies omit the sequence field:

```
+-----------+-----------+
| kind (1B) | payload   |
+-----------+-----------+
```

| Field   | Type | Size    | Description                                    |
|---------|------|---------|------------------------------------------------|
| kind    | u8   | 1 byte  | Message type discriminant (see "Response Messages") |
| payload | ...  | 0..N    | Variant-specific fields                        |

### Node Frames

These frames carry no exchange body. The node sends and reads them itself; a client tells them apart by their tag.

| Tag  | Name              | Direction        | Body                               |
|------|-------------------|------------------|------------------------------------|
| 0x01 | Heartbeat         | Server to client | none                               |
| 0x02 | BatchEnd          | Server to client | none                               |
| 0x03 | EngineError       | Server to client | none                               |
| 0x04 | ServerBusy        | Server to client | none                               |
| 0x05 | Challenge         | Server to client | nonce (32 bytes)                   |
| 0x06 | ChallengeResponse | Client to server | signature (64) + public key (32)   |
| 0x07 | AuthFailed        | Server to client | none                               |
| 0x08 | ServerReady       | Server to client | none                               |
| 0x09 | *(exchange message)* | Both          | a request or response body         |

- **Heartbeat**: server-initiated keepalive sent to idle connections (see "Heartbeat and Keepalive").
- **BatchEnd**: the last frame of the reply to one request (see "BatchEnd Semantics").
- **EngineError**: the matching engine hit an internal error on the request. The batch still ends with a `BatchEnd`.
- **ServerBusy**: the server's input pipeline is full and the request was not accepted. Retry after a brief backoff. The reader answers this directly, without entering the pipeline, so the server can always respond even when saturated.
- **Challenge**, **ChallengeResponse**, **AuthFailed**, **ServerReady**: the authentication handshake (see "Authentication Handshake").

---

## Type Reference

These types appear throughout the field layouts below:

| Type       | Wire size | Encoding                              |
|------------|-----------|---------------------------------------|
| Symbol     | 4 bytes   | u32 LE instrument identifier          |
| OrderId    | 8 bytes   | u64 LE                                |
| AccountId  | 4 bytes   | u32 LE                                |
| CurrencyId | 4 bytes   | u32 LE                                |
| Price      | 8 bytes   | u64 LE (NonZeroU64, must not be zero) |
| Quantity   | 8 bytes   | u64 LE (NonZeroU64, must not be zero) |
| Side       | 1 byte    | 0 = Buy, 1 = Sell                     |
| TimeInForce| 1 byte    | 0 = GTC, 1 = IOC, 2 = FOK, 3 = Day, 4 = GTD |
| SelfTradePrevention | 1 byte | 0 = Allow, 1 = CancelNewest, 2 = CancelOldest, 3 = CancelBoth |

### Order Encoding

Orders are encoded inline within `SubmitOrder` requests. The layout is variable-length because the order type fields differ:

```
id(8) + account(4) + side(1) + order_type(1) + order_type_fields(0..16) + tif(1) + quantity(8) + stp(1) + [expiry_ns(8)]
```

| Offset | Field           | Size   | Notes                                    |
|--------|-----------------|--------|------------------------------------------|
| 0      | id              | 8      | OrderId, u64 LE                          |
| 8      | account         | 4      | AccountId, u32 LE                        |
| 12     | side            | 1      | 0=Buy, 1=Sell                            |
| 13     | order_type      | 1      | See below                                |
| 14     | order_type_data | 0..16  | Variable, depends on order_type          |
| ...    | time_in_force   | 1      | 0=GTC, 1=IOC, 2=FOK, 3=Day, 4=GTD        |
| ...    | quantity        | 8      | u64 LE (NonZeroU64)                      |
| ...    | stp             | 1      | Self-trade prevention mode               |
| ...    | expiry_ns       | 0 or 8 | Only present for GTD (tif=4): u64 LE nanoseconds since Unix epoch |

**Order types and fields**:

| Value | Type           | Extra fields                                   | Extra size |
|-------|----------------|------------------------------------------------|------------|
| 0     | Market         | (none)                                         | 0 bytes    |
| 1     | Limit          | price (u64 LE)                                 | 8 bytes    |
| 2     | Stop           | trigger_price (u64 LE)                         | 8 bytes    |
| 3     | StopLimit      | trigger_price (u64 LE) + limit_price (u64 LE)  | 16 bytes   |
| 4     | Limit PostOnly | price (u64 LE)                                 | 8 bytes    |

Total order size: 24 bytes (Market, no expiry) to 48 bytes (StopLimit + GTD expiry).

---

## Request Messages (Client to Server)

| Kind | Name              | Permission     | Payload size         |
|------|-------------------|----------------|----------------------|
| 0x10 | SubmitOrder       | Trader         | 4 + 24..48 (variable)|
| 0x11 | CancelOrder       | Trader         | 16                   |
| 0x12 | Heartbeat         | Any            | 0                    |
| 0x13 | CancelAll         | Trader         | 4                    |
| 0x14 | CancelReplace     | Trader         | 32                   |
| 0x15 | AddInstrument     | Operator       | 12                   |
| 0x16 | Deposit           | Custodian      | 16                   |
| 0x17 | Withdraw          | Custodian      | 16                   |
| 0x18 | SetRiskLimits     | Operator       | 5..21 (variable)     |
| 0x19 | SetCircuitBreaker | Operator       | 5..21 (variable)     |
| 0x1A | SetFeeSchedule    | Operator       | 8                    |
| 0x1B | EndOfDay          | Operator       | 0                    |
| 0x1C | DisableInstrument | Operator       | 4                    |
| 0x1D | EnableInstrument  | Operator       | 4                    |
| 0x1E | RemoveInstrument  | Operator       | 4                    |
| 0x1F | Subscribe         | Any (event port) | 1 + count×4        |
| 0x20 | QueryStats        | Operator       | 0                    |
| 0x21 | QueryPosition     | Trader         | 4                    |
| 0x22 | QueryRequestSeq   | Trader         | 0                    |

Payload sizes above exclude the 1-byte kind and 8-byte seq. The frame length = 1 (frame tag) + 1 (kind) + 8 (seq) + payload size.

### 0x10: SubmitOrder

| Offset | Field  | Size     |
|--------|--------|----------|
| 0      | symbol | 4 (u32)  |
| 4      | order  | 24..48   |

The order is encoded inline per the Order Encoding section above.

### 0x11: CancelOrder

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | symbol   | 4 (u32)  |
| 4      | account  | 4 (u32)  |
| 8      | order_id | 8 (u64)  |

### 0x12: Heartbeat

No payload. Resets the server's idle timeout for this connection and is otherwise dropped on arrival: it never reaches the matching engine and gets no reply.

### 0x13: CancelAll

| Offset | Field   | Size     |
|--------|---------|----------|
| 0      | account | 4 (u32)  |

Kill switch: cancels all resting orders and pending stops for the given account across all instruments.

### 0x14: CancelReplace

| Offset | Field        | Size     |
|--------|--------------|----------|
| 0      | symbol       | 4 (u32)  |
| 4      | account      | 4 (u32)  |
| 8      | order_id     | 8 (u64)  |
| 16     | new_price    | 8 (u64)  |
| 24     | new_quantity | 8 (u64)  |

Atomically amends a resting limit order's price and quantity. Both `new_price` and `new_quantity` must be NonZeroU64. If the amendment fails, the original order remains intact.

### 0x15: AddInstrument

| Offset | Field  | Size     |
|--------|--------|----------|
| 0      | symbol | 4 (u32)  |
| 4      | base   | 4 (u32)  |
| 8      | quote  | 4 (u32)  |

Registers a new instrument with its base and quote currency identifiers.

### 0x16: Deposit

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | account  | 4 (u32)  |
| 4      | currency | 4 (u32)  |
| 8      | amount   | 8 (u64)  |

Credits funds to an account.

### 0x17: Withdraw

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | account  | 4 (u32)  |
| 4      | currency | 4 (u32)  |
| 8      | amount   | 8 (u64)  |

Debits funds from an account. Rejects with `HasRestingOrders` if the account has resting orders (must `CancelAll` first). Rejects with `InsufficientBalance` if the account lacks funds. Removes the balance entry when it reaches zero.

### 0x18: SetRiskLimits

| Offset | Field                | Size     | Notes                           |
|--------|----------------------|----------|---------------------------------|
| 0      | symbol               | 4 (u32)  |                                 |
| 4      | flags                | 1        | Bitmask (see below)             |
| 5      | max_order_qty        | 0 or 8   | Present if flags bit 0 is set   |
| 5 or 13| max_order_notional   | 0 or 8   | Present if flags bit 1 is set   |

Flags byte:
- Bit 0: has `max_order_qty` (u64 LE, NonZeroU64)
- Bit 1: has `max_order_notional` (u64 LE)

Omitted fields clear the corresponding limit.

### 0x19: SetCircuitBreaker

| Offset | Field            | Size     | Notes                           |
|--------|------------------|----------|---------------------------------|
| 0      | symbol           | 4 (u32)  |                                 |
| 4      | flags            | 1        | Bitmask (see below)             |
| 5      | price_band_lower | 0 or 8   | Present if flags bit 0 is set   |
| 5 or 13| price_band_upper | 0 or 8   | Present if flags bit 1 is set   |

Flags byte:
- Bit 0: has `price_band_lower` (u64 LE, NonZeroU64)
- Bit 1: has `price_band_upper` (u64 LE, NonZeroU64)
- Bit 2: `halted` (1 = trading halted, 0 = not halted)

### 0x1A: SetFeeSchedule

| Offset | Field          | Size     |
|--------|----------------|----------|
| 0      | symbol         | 4 (u32)  |
| 4      | maker_fee_bps  | 2 (i16)  |
| 6      | taker_fee_bps  | 2 (i16)  |

Fee values are in basis points (1 bps = 0.01%). Negative values are rebates (exchange pays the maker/taker). Range: -10000 to 10000.

### 0x1B: EndOfDay

No payload. Cancels all resting orders and pending stops with `TimeInForce::Day` across all instruments. Triggered by an operator at end-of-session.

### 0x1C: DisableInstrument

| Offset | Field  | Size     |
|--------|--------|----------|
| 0      | symbol | 4 (u32)  |

Disables an instrument: rejects new orders and cancels all resting orders and pending stops. Re-enable is possible.

### 0x1D: EnableInstrument

| Offset | Field  | Size     |
|--------|--------|----------|
| 0      | symbol | 4 (u32)  |

Re-enables a previously disabled instrument for trading.

### 0x1E: RemoveInstrument

| Offset | Field  | Size     |
|--------|--------|----------|
| 0      | symbol | 4 (u32)  |

Permanently removes a disabled instrument. Only succeeds if the instrument is disabled and has no resting orders.

### 0x1F: Subscribe

Sent to the event publisher's port after authentication, by the market-data gateway or any other subscriber. Requests book snapshots and a live event firehose for the listed symbols. On the trading port it is dropped on arrival.

| Offset | Field   | Size          |
|--------|---------|---------------|
| 0      | count   | 1 (u8)        |
| 1      | symbols | count×4 (u32) |

`count = 0` subscribes to all symbols. Maximum 8 symbols per request.

### 0x20: QueryStats

No payload. Requests a server stats snapshot. Response is a `StatsHeader` followed by `BatchEnd`.

### 0x21: QueryPosition

| Offset | Field   | Size     |
|--------|---------|----------|
| 0      | account | 4 (u32)  |

Queries an account's balances. Response is a `PositionSnapshot` followed by `BatchEnd`.

### 0x22: QueryRequestSeq

No payload. Asks for the high-water mark the engine keeps for this connection's key, so a client can resume its `seq` counter after a reconnect. Response is a `RequestSeqHwm` followed by `BatchEnd`.

---

## Response Messages (Server to Client)

| Kind | Name                     | Payload size |
|------|--------------------------|--------------|
| 0x30 | Placed                   | 33           |
| 0x31 | Fill                     | 60           |
| 0x32 | Cancelled                | 24           |
| 0x33 | Triggered                | 24           |
| 0x34 | Rejected                 | 17           |
| 0x35 | Replaced                 | 49           |
| 0x36 | InstrumentStatusChanged  | 5            |
| 0x37 | StatsHeader              | 24           |
| 0x38 | BookSnapshotBegin        | 12           |
| 0x39 | BookSnapshotLevel        | 25           |
| 0x3A | BookSnapshotEnd          | 8            |
| 0x3B | SnapshotComplete         | 8            |
| 0x3C | PositionSnapshot         | 5 + count×20 |
| 0x3D | RequestSeqHwm            | 8            |

Payload sizes above exclude the 1-byte kind. `BatchEnd`, `EngineError`, `ServerBusy` and the server's heartbeat are node frames (see "Node Frames").

### 0x30: Placed

Confirms a limit order was placed on the book (resting).

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | order_id | 8 (u64)  |
| 8      | symbol   | 4 (u32)  |
| 12     | account  | 4 (u32)  |
| 16     | side     | 1        |
| 17     | price    | 8 (u64)  |
| 25     | quantity | 8 (u64)  |

### 0x31: Fill

Reports a trade execution between a maker and taker.

| Offset | Field          | Size     |
|--------|----------------|----------|
| 0      | maker_order_id | 8 (u64)  |
| 8      | taker_order_id | 8 (u64)  |
| 16     | symbol         | 4 (u32)  |
| 20     | maker_account  | 4 (u32)  |
| 24     | taker_account  | 4 (u32)  |
| 28     | price          | 8 (u64)  |
| 36     | quantity       | 8 (u64)  |
| 44     | maker_fee      | 8 (i64)  |
| 52     | taker_fee      | 8 (i64)  |

Fees are signed: positive = fee charged, negative = rebate credited. Both values are in quote currency.

### 0x32: Cancelled

Confirms an order was cancelled.

| Offset | Field              | Size     |
|--------|--------------------|----------|
| 0      | order_id           | 8 (u64)  |
| 8      | symbol             | 4 (u32)  |
| 12     | account            | 4 (u32)  |
| 16     | remaining_quantity | 8 (u64)  |

### 0x33: Triggered

Reports that a stop order was triggered (converted to a market/limit order).

| Offset | Field         | Size     |
|--------|---------------|----------|
| 0      | order_id      | 8 (u64)  |
| 8      | symbol        | 4 (u32)  |
| 12     | account       | 4 (u32)  |
| 16     | trigger_price | 8 (u64)  |

### 0x34: Rejected

Reports that an order was rejected by the matching engine.

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | order_id | 8 (u64)  |
| 8      | symbol   | 4 (u32)  |
| 12     | account  | 4 (u32)  |
| 16     | reason   | 1        |

**Reject reason codes**:

| Code | Reason                |
|------|-----------------------|
| 0    | NoLiquidity           |
| 1    | FOKCannotFill         |
| 2    | InsufficientBalance   |
| 3    | UnknownAccount        |
| 4    | UnknownSymbol         |
| 5    | SelfTradePrevented    |
| 6    | DuplicateOrderId      |
| 7    | ExceedsMaxOrderQty    |
| 8    | ExceedsMaxNotional    |
| 9    | TradingHalted         |
| 10   | OutsidePriceBand      |
| 11   | UnknownOrder          |
| 12   | PriceWouldCross       |
| 13   | PostOnlyWouldCross    |
| 14   | HasRestingOrders      |
| 15   | DuplicateRequest      |
| 16   | ReplicaDisconnected   |
| 17   | InvalidExpiry         |
| 18   | InstrumentDisabled    |
| 19   | ExceedsMaxOpenOrders  |
| 20   | ExceedsOrderRate      |
| 21   | *(reserved)*          |

### 0x35: Replaced

Confirms a cancel-replace amendment succeeded.

| Offset | Field         | Size     |
|--------|---------------|----------|
| 0      | order_id      | 8 (u64)  |
| 8      | symbol        | 4 (u32)  |
| 12     | account       | 4 (u32)  |
| 16     | side          | 1        |
| 17     | old_price     | 8 (u64)  |
| 25     | new_price     | 8 (u64)  |
| 33     | old_remaining | 8 (u64)  |
| 41     | new_remaining | 8 (u64)  |

### 0x36: InstrumentStatusChanged

Reports a change in instrument lifecycle status.

| Offset | Field  | Size     |
|--------|--------|----------|
| 0      | symbol | 4 (u32)  |
| 4      | status | 1        |

**Status codes**: 0 = Enabled, 1 = Disabled, 2 = Removed.

### 0x37: StatsHeader

Server stats snapshot, sent in response to `QueryStats`.

| Offset | Field              | Size     |
|--------|--------------------|----------|
| 0      | active_connections | 8 (u64)  |
| 8      | events_processed   | 8 (u64)  |
| 16     | journal_sequence   | 8 (u64)  |

### 0x38: BookSnapshotBegin

Start of a book snapshot for one symbol. Sent by the event publisher during the Subscribe handshake.

| Offset | Field            | Size     |
|--------|------------------|----------|
| 0      | symbol           | 4 (u32)  |
| 4      | last_applied_seq | 8 (u64)  |

### 0x39: BookSnapshotLevel

One price level in a book snapshot.

| Offset | Field       | Size     |
|--------|-------------|----------|
| 0      | symbol      | 4 (u32)  |
| 4      | side        | 1        |
| 5      | price       | 8 (u64)  |
| 13     | qty         | 8 (u64)  |
| 21     | order_count | 4 (u32)  |

### 0x3A: BookSnapshotEnd

End of a book snapshot for one symbol.

| Offset | Field       | Size     |
|--------|-------------|----------|
| 0      | symbol      | 4 (u32)  |
| 4      | level_count | 4 (u32)  |

### 0x3B: SnapshotComplete

All requested book snapshots have been sent. The firehose resumes from `last_applied_seq + 1`.

| Offset | Field            | Size     |
|--------|------------------|----------|
| 0      | last_applied_seq | 8 (u64)  |

### 0x3C: PositionSnapshot

Account balance snapshot in response to `QueryPosition`.

| Offset | Field    | Size          |
|--------|----------|---------------|
| 0      | account  | 4 (u32)       |
| 4      | count    | 1 (u8)        |
| 5      | balances | count×20      |

Each balance entry (20 bytes):

| Offset | Field    | Size     |
|--------|----------|----------|
| 0      | currency | 4 (u32)  |
| 4      | free     | 8 (u64)  |
| 12     | reserved | 8 (u64)  |

Maximum 16 entries per snapshot (capped by the engine).

### 0x3D: RequestSeqHwm

The request-sequence high-water mark for the connection's key, in response to `QueryRequestSeq`. `0` for a key the engine has never seen.

| Offset | Field | Size     |
|--------|-------|----------|
| 0      | hwm   | 8 (u64)  |

---

## Authentication Handshake

Every connection must complete an Ed25519 challenge-response handshake before sending any trading or admin requests. The handshake runs on the accept thread (cold path), not the matching engine hot path. Its frames are all node frames.

### Flow

```
Client                              Server
  |                                    |
  |  <--- TCP/UDS connect --->         |
  |                                    |
  |  Challenge (tag=0x05, 32B nonce)   |
  |  <---------------------------------|  Server generates 32 random bytes
  |                                    |
  |  ChallengeResponse (tag=0x06)      |
  |  sig(64B) + pubkey(32B)            |
  |  --------------------------------->|  Client signs nonce with Ed25519 key
  |                                    |
  |         [verify signature]         |
  |         [lookup pubkey in          |
  |          authorized_keys]          |
  |                                    |
  |    ServerReady (tag=0x08)          |
  |  <---------------------------------|  Auth succeeded, normal operation begins
  |                                    |
  |  --- OR ---                        |
  |                                    |
  |    AuthFailed (tag=0x07)           |
  |  <---------------------------------|  Auth failed, connection dropped
```

### Timeout

The server sets a **5-second read timeout** on the socket during the auth handshake. If the client does not send a `ChallengeResponse` within 5 seconds, the connection is closed. The timeout is cleared after successful authentication.

### Frame size

The maximum accepted auth frame size is 256 bytes. The expected `ChallengeResponse` frame is 97 bytes after its length prefix (1 tag + 64 signature + 32 public key).

### Post-auth behavior

After authentication, a `ChallengeResponse`, like any frame under a tag other than `0x09`, is dropped without a reply.

---

## Permission Model

Permission levels are assigned per public key in the `authorized_keys` file and checked on the reader thread (zero cost on the hot path).

### Permission levels

| Level       | Trading | Operator (Config) | Fund Mgmt | Heartbeat |
|-------------|---------|-------------------|-----------|-----------|
| Operator    | No      | Yes               | No        | Yes       |
| Trader      | Yes     | No                | No        | Yes       |
| Custodian   | No      | No                | Yes       | Yes       |
| ReadOnly    | No      | No                | No        | Yes       |
| Replication | --      | --                | --        | --        |

**Trading operations** (require `Trader`):
- SubmitOrder, CancelOrder, CancelAll, CancelReplace, QueryPosition, QueryRequestSeq

**Operator operations** (require `Operator`):
- AddInstrument, SetRiskLimits, SetCircuitBreaker, SetFeeSchedule, QueryStats, EndOfDay, DisableInstrument, EnableInstrument, RemoveInstrument

**Fund management operations** (require `Custodian`):
- Deposit, Withdraw

**Replication** (require `Replication`):
- Used for replica-to-primary connections only. The trading port and the event publisher's port refuse a replication key during the handshake, so it cannot open a client connection at all.

**Universal operations** (any client role — every level but `Replication`):
- Heartbeat, and Subscribe on the event publisher's port

Requests that fail the permission check are dropped on the reader thread and never reach the matching engine.

### Authorized keys file format

```
# <permission> <base64-public-key> <optional-comment>
operator AAAA...base64...= ops-team
trader BBBB...base64...= market-maker-1
custodian CCCC...base64...= treasury
readonly DDDD...base64...= monitoring
replication EEEE...base64...= replica-1
```

Lines starting with `#` and empty lines are ignored. Public keys are 32-byte Ed25519 keys encoded in standard base64. Each key is listed once: a file that lists a key twice, under the same role or another, is refused, and the node does not start. The error names the second line.

---

## Per-Key Idempotency

Every request body includes a `seq` field (u64) -- a per-key monotonic sequence number. The engine tracks a high-water mark per authentication key (identified by a hash of the public key). If a request arrives with `seq <= hwm`, it is rejected with `DuplicateRequest` before it touches any state.

This makes retries safe: if a client sends an order, loses the connection before receiving the response, and reconnects with the same key, it can safely retry with the same `seq`. If the original request was already processed, the retry is rejected as a duplicate. If it wasn't processed (the server crashed before journaling it), the retry succeeds normally. A client that has lost its counter reads the mark back with `QueryRequestSeq`.

The `seq` is journaled with the request, so the check reaches the same verdict everywhere the request is applied: on a recovery from the journal, on a replica following the primary, and in the copy of the engine that snapshots are written from. The HWM itself is part of every snapshot. Heartbeats and the read-only queries (`QueryStats`, `QueryPosition`, `QueryRequestSeq`) are exempt from the check; the `ChallengeResponse` carries no `seq` at all.

---

## Heartbeat and Keepalive

### Client-to-server heartbeat (kind 0x12)

A request with no payload. Any data the server receives, heartbeats included, resets the connection's idle timer. Heartbeat requests do **not** enter the pipeline -- they are dropped on the reader thread.

### Server-to-client heartbeat (tag 0x01)

The response stage sends a heartbeat node frame to connections that have been idle for the configured interval. Default: **10 seconds** (`--heartbeat-interval-secs`).

### Connection timeout

If no data is received from a client within the configured window, the connection is closed. Default: **30 seconds** (`--connection-timeout-secs`). Set to 0 to disable. The timeout is checked approximately once per second via a coarse scan to avoid overhead during high throughput.

Clients should send heartbeat requests at an interval shorter than the connection timeout to prevent disconnection during idle periods.

---

## BatchEnd Semantics

A single request can produce multiple response messages. For example, a `SubmitOrder` that crosses multiple resting orders produces:

1. One or more **Fill** reports (one per price level matched)
2. Possibly a **Placed** report (if the order partially fills and the remainder rests)
3. Possibly a **Rejected** report (if the order is rejected)
4. A **BatchEnd** to signal completion

The **BatchEnd** node frame (tag `0x02`) tells the client that all reports for the preceding request have been sent. This allows pipelined clients to correlate responses with requests: after sending N requests, the client reads responses until it receives N BatchEnd markers.

For requests that produce a single response (e.g., `CancelOrder` produces one `Cancelled` or `Rejected`), BatchEnd still follows to maintain the uniform protocol.

---

## Byte-Level Encoding Examples

### Example 1: Heartbeat Request

The simplest possible request -- kind and seq, no payload. Seq is 0 (heartbeats are exempt from dedup).

```
Frame (14 bytes total):
  [0A 00 00 00]   length = 10 (LE u32: 1 tag + 1 kind + 8 seq)
  [09]            tag = 0x09 (exchange message)
  [12]            kind = 0x12 (Heartbeat)
  [00 00 00 00    seq = 0 (LE u64)
   00 00 00 00]
```

### Example 2: CancelOrder Request

Cancel order ID 42 on symbol 1, account 5, request seq 7.

```
Frame (30 bytes total):
  [1A 00 00 00]   length = 26 (LE u32: 1 tag + 1 kind + 8 seq + 16 payload)
  [09]            tag = 0x09 (exchange message)
  [11]            kind = 0x11 (CancelOrder)
  [07 00 00 00    seq = 7 (LE u64)
   00 00 00 00]
  [01 00 00 00]   symbol = 1 (LE u32)
  [05 00 00 00]   account = 5 (LE u32)
  [2A 00 00 00    order_id = 42 (LE u64)
   00 00 00 00]
```

### Example 3: BatchEnd (node frame)

```
Frame (5 bytes total):
  [01 00 00 00]   length = 1 (LE u32)
  [02]            tag = 0x02 (BatchEnd)
```

### Example 4: Challenge (node frame, server to client)

```
Frame (37 bytes total):
  [21 00 00 00]   length = 33 (LE u32: 1 tag + 32 nonce)
  [05]            tag = 0x05 (Challenge)
  [xx xx ... xx]  nonce (32 random bytes)
```

---

## Error Handling

- **Truncated bodies**: A body shorter than its kind requires is malformed.
- **Unknown kinds**: A body whose first byte is not a known kind is malformed.
- **Invalid fields**: Zero values in NonZeroU64 fields (prices, quantities) and out-of-range enum values are malformed.
- **Malformed requests** are dropped on the reader thread without a reply; the server logs them at debug level and keeps the connection open.
- **Oversized frames**: Frames with length > 1024 bytes are rejected at the framing layer and the connection is closed.

---

## Source Files

- `crates/exchange/protocol/src/codec.rs` -- encode/decode functions, kind constants, field layouts
- `crates/exchange/protocol/src/message.rs` -- `Request` and `ResponseKind` enum definitions
- `crates/exchange/types/src/le.rs` -- shared little-endian helpers and enum encoding (Side, TimeInForce, SelfTradeProtection)
- The frame tags, the length-prefixed framing and the authentication handshake belong to the Melin sequencer's `melin-wire-protocol` and `melin-client` crates.
