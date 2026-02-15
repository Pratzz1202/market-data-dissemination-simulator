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

Snapshot is full published state at sequence `snapshot_seq`.

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

## Recovery

- Client-triggered: explicit `ResyncRequest`.
- Server-triggered: unsolicited reset snapshots (simulated reset events and backpressure recovery).

