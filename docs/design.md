# Design

## Overview

MDD-Sim uses a single gRPC bidirectional stream per client. Server-side simulator threads generate per-instrument order book updates, assign strict per-instrument sequence numbers, and fan out incrementals to subscribed clients. On subscription or resync, server sends a full snapshot representing exact state at `snapshot_seq`.

## Components

- `Simulator`
  - Deterministic RNG per instrument.
  - Maintains internal `OrderBook` and per-instrument sequence counter.
  - Generates incrementals and periodic unsolicited reset snapshots.
- `SubscriptionManager`
  - Tracks client subscriptions and requested depth.
- `Publisher`
  - Routes updates to subscribed clients.
  - Enforces bounded per-client/per-instrument queues.
  - Implements backpressure strategy: drop newest incremental + mark dirty + force reset snapshot.
- `MarketDataServiceImpl`
  - Handles client stream commands (subscribe/unsubscribe/resync/ping/hello).
  - Integrates simulator, subscriptions, publisher, recorder.
- `ClientSession`
  - Manages long-lived stream with read/write loops.
  - Supports automatic reconnect and re-subscribe.
- `ApplyEngine`
  - Applies snapshots/incrementals.
  - Validates `prev_seq == last_seq` invariant.
  - Triggers resync on gaps, out-of-order, duplicates, or apply failures.

## Data Flow

1. Simulator thread generates update for `instrument_id`.
2. Simulator applies update to server book and emits incremental `(seq, prev_seq)`.
3. Publisher fans out to subscribers.
4. Slow clients may drop incrementals, get marked dirty.
5. Next cycle sends reset snapshot to dirty client before further incrementals.
6. Client apply engine reconstructs book and sequence state.
7. On mismatch, client sends `ResyncRequest`; server returns snapshot reset.

## Correctness Model

- Snapshot replaces local book and sets `last_seq = snapshot_seq`.
- Incremental applies iff `prev_seq == last_seq` and `seq > last_seq`.
- Any mismatch forces resync and incremental is rejected.
- Per-instrument sequence monotonicity is guaranteed by simulator lock + counter.

## Reconnect Semantics

- Client automatically reconnects when stream ends.
- Client re-sends `Hello` and all active subscriptions.
- Server treats reconnect as fresh stream and sends snapshots as part of subscribe flow.
- Current policy: snapshot-on-reconnect (no delta catch-up ring buffer).

## Backpressure Policy

Policy A implemented:

- Queue bound per `(client, instrument)`.
- Overflow behavior for incrementals:
  - drop incoming incremental (drop-newest policy)
  - mark `(client, instrument)` dirty
- Recovery behavior:
  - enqueue reset snapshot
  - clear dirty flag
  - skip stale incremental for that cycle

This prevents OOM and gives deterministic recovery semantics.

## Record/Replay

- `Recorder` stores a binary length-prefixed stream of protobuf `RecordedEvent`.
- `mdd_replay` re-applies events and verifies sequence continuity.
- Determinism comes from fixed seed + deterministic update logic.

## Observability

Structured JSON logs include:

- lifecycle: `server_start`, `client_connected`, `client_disconnected`
- protocol: `subscribe`, `snapshot_sent`, `incremental_sent`, `resync_served`
- resilience: `gap_detected`, `resync_requested`, `backpressure_drop`
- diagnostics: `simulation_error`, `server_error`

Metrics registry tracks:

- gauge: `connected_clients`
- counters: `total_snapshots`, `total_incrementals`, `total_resyncs`, `total_drops`

## Operational Behavior

- Graceful shutdown:
  - signal received -> stop simulator threads -> shutdown gRPC server.
- Config validation rejects malformed instrument settings early.

