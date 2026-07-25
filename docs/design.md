# Design

## Overview

MDD-Sim uses a single gRPC bidirectional stream per client. Server-side
simulator threads generate per-instrument order book updates, assign strict
per-instrument sequence numbers, and fan out incrementals to subscribed
clients. On subscription or resync, the server sends a full-book snapshot
representing exact state at `snapshot_seq`.

## Components

- `Simulator`
  - Deterministic RNG per instrument.
  - Maintains internal `OrderBook` and per-instrument sequence counter.
  - Per-instrument mutexes: instruments are independent, so simulator threads
    never contend with each other.
  - Generates incrementals and periodic unsolicited reset snapshots.
- `SubscriptionManager`
  - Tracks client subscriptions and requested depth.
- `Publisher`
  - Routes updates to subscribed clients.
  - Enforces bounded per-client/per-instrument queues for incrementals;
    control messages bypass the bound (dropping them would lose errors,
    pongs, and unsubscribe confirmations).
  - Backpressure strategy: drop newest incremental, mark dirty, replace the
    queue with a reset snapshot on the next publish cycle.
  - Reports per-client lag through a metrics provider sampled at scrape time,
    keeping the publish hot path free of registry locking.
- `MarketDataServiceImpl`
  - Handles client stream commands (subscribe/unsubscribe/resync/ping/hello).
  - Owns one publish mutex per instrument: snapshot delivery (subscribe,
    resync) is serialized against incremental fan-out, so a snapshot built at
    seq S can never race a concurrently generated incremental S+1 out of the
    queue (that race previously caused a spurious gap+resync right after
    subscribing).
  - Integrates simulator, subscriptions, publisher, recorder.
- `ClientSession`
  - Manages the long-lived stream with read/write loops.
  - Automatic reconnect and re-subscribe. The connected flag flips under the
    same lock that snapshots the desired-subscription set, so a subscribe
    issued during connection setup is either replayed with the initial set or
    enqueued — never silently lost.
  - Callbacks are installed before `Start()` and invoked from the read thread
    without locking.
- `ApplyEngine`
  - Applies snapshots/incrementals.
  - Validates `prev_seq == last_seq` and `seq > last_seq`.
  - Triggers resync on gaps, out-of-order, duplicates, or apply failures.

## Snapshot depth

Snapshots always carry the full book. A depth-truncated snapshot combined
with full-book incrementals silently desyncs clients: when a remove uncovers
a level the snapshot never contained, the client's top-of-book diverges from
the server's while sequence numbers remain perfectly valid, so no resync
ever fires. `requested_depth` is retained as a client display preference
only, and the config field `publish_depth` no longer truncates protocol
snapshots either — it serves as the default for `levels_per_side`
(`2 * publish_depth` when unset) and as a display-depth hint. The
`TestClientBookMatchesServerBook` integration test locks this in.

## Data flow

1. The instrument thread takes the instrument's publish lock, generates an
   update, applies it to the server book, and assigns `(prev_seq, seq)`.
2. The incremental is recorded (if recording), then fanned out to
   subscriber queues.
3. Slow clients overflow their queue: the incremental is dropped for them and
   the queue marked dirty.
4. On the next cycle a dirty queue is cleared and replaced with a reset
   snapshot; the client resumes from `snapshot_seq`.
5. The client apply engine reconstructs the book; on any sequence mismatch it
   requests a resync and the server answers with a full snapshot.

## Correctness model

- Snapshot replaces the local book and sets `last_seq = snapshot_seq`.
- An incremental applies iff `prev_seq == last_seq` and `seq > last_seq`.
- Any mismatch forces a resync; the incremental is rejected.
- Per-instrument sequence monotonicity is guaranteed by the per-instrument
  simulator lock and counter; cross-instrument ordering is not defined.

## Reconnect semantics

- The client reconnects automatically when the stream ends, re-sends `Hello`
  and all desired subscriptions, and clears any outbound messages queued for
  the previous stream (the desired set already covers them).
- The server treats a reconnect as a fresh stream and sends snapshots as part
  of the subscribe flow. Policy: snapshot-on-reconnect, no delta catch-up
  ring buffer.

## Backpressure policy

- Queue bound per `(client, instrument)`, incrementals only.
- Overflow: drop the incoming incremental (drop-newest), mark dirty.
- Recovery: enqueue a reset snapshot (replacing the stale queue), clear the
  dirty flag, skip incrementals for that instrument until the snapshot is
  queued.
- Control messages are exempt from the bound; they are rare, small, and
  losing them breaks protocol bookkeeping rather than saving memory.

This bounds memory and yields deterministic recovery semantics.

## Record/replay

- `Recorder` writes a length-prefixed (little-endian, portable) stream of
  protobuf `RecordedEvent`.
- Recording starts with a baseline snapshot per instrument, so a fresh
  recording replays without an artificial leading gap.
- The record stream captures generated truth: `--drop_every_n` suppresses
  publishing, not recording, so strict replay stays gap-free while clients
  exercise the resync path.
- `mdd_replay` re-applies events, verifies `prev_seq` continuity, and
  validates final book integrity; `--strict` exits non-zero on violations.
- Determinism comes from the fixed seed + deterministic update logic
  (`test_replay_determinism` verifies identical streams for identical seeds).

## Observability

Structured JSON-lines logs (control characters escaped). Levels: `info` for
lifecycle/protocol events, `debug` for per-update events (`incremental_sent`,
`incremental_applied`, `backpressure_drop`). The debug level is checked
before any formatting work, so the publish path pays nothing when disabled
(server default). Event names:

- lifecycle: `server_start`, `client_connected`, `client_disconnected`
- protocol: `subscribe`, `snapshot_sent`, `resync_served`
- resilience: `gap_detected`, `resync_requested`, `loss_simulation_drop`
- diagnostics: `simulation_error`
- debug: `incremental_sent`, `incremental_applied`, `backpressure_drop`

Metrics (Prometheus text on `/metrics`, `mdd_` prefix): connected clients,
snapshot/incremental counters, resyncs, backpressure and simulated-loss
drops, scrape-windowed incremental rate, and per-client lag sampled at
scrape time.

## Operational behavior

- Graceful shutdown: signal -> stop simulator threads -> shutdown gRPC server.
- Config validation rejects malformed instrument settings early (missing
  required fields, non-positive tick/price, negative volatility, duplicate
  instrument ids).
