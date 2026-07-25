# Protocol

## Service

`MarketDataService.Stream(stream ClientMsg) returns (stream ServerMsg)`

One long-lived stream per connection.

## Versioning and Compatibility

- Client sends `ClientHello` with:
  - `schema_version`
  - `client_build_id`
  - `capabilities[]`
- Server responds with `ServerHello`:
  - `schema_version`
  - `server_build_id`
  - `capabilities[]`

Compatibility policy:

- Unknown fields are ignored (protobuf forward compatibility).
- If client schema is unsupported, server emits `Error(code=UNSUPPORTED_SCHEMA)`.

## Client Messages

- `Subscribe { instrument_id, requested_depth, subscription_id }`
- `Unsubscribe { instrument_id }`
- `ResyncRequest { instrument_id, reason, last_seq_seen }`
- `Ping { client_timestamp_ns }`

## Server Messages

- `Snapshot { instrument_id, snapshot_seq, bids[], asks[], is_reset, server_timestamp_ns, reason }`
- `Incremental { instrument_id, seq, prev_seq, updates[], server_timestamp_ns }`
- `Unsubscribed { instrument_id, reason }`
- `Error { instrument_id, code, message }`
- `Pong { client_timestamp_ns, server_timestamp_ns }`

## Snapshot Semantics

A snapshot carries the **full book** at sequence `snapshot_seq`. Snapshots
are never depth-truncated: incrementals address the whole book, so a partial
snapshot would let the client book silently diverge the moment a remove
uncovers a level the snapshot never contained (sequence numbers would stay
valid and no resync would fire). `requested_depth` on `Subscribe` is a
client-side display preference only.

Client action:

- Replace local book with snapshot levels.
- Set `last_seq = snapshot_seq`.
- Clear gap/resync pending state.

## Incremental Semantics

Client may apply incremental only when:

- `prev_seq == last_seq`
- `seq > last_seq`

After apply:

- `last_seq = seq`

If mismatch:

- treat as gap/out-of-order/duplicate
- request resync
- do not apply incremental

## Unsubscribe Semantics

After `Unsubscribed`, client:

- marks instrument unsubscribed
- clears local book and sequence state

## Error Handling

Common server error codes:

- `UNKNOWN_INSTRUMENT`
- `ALREADY_SUBSCRIBED`
- `NOT_SUBSCRIBED`
- `UNSUPPORTED_SCHEMA`
- `EMPTY_MESSAGE`

## Delivery Guarantees

- Incrementals may be dropped under backpressure (bounded per-instrument
  queues); the affected instrument recovers via a reset snapshot.
- Control messages (`Error`, `Pong`, `Unsubscribed`, `ServerHello`) are never
  dropped by queue bounds.

## Recovery

- Client-triggered: explicit `ResyncRequest`.
- Server-triggered: unsolicited reset snapshots (simulated reset events and backpressure recovery).

