# Market Data Dissemination Simulator (MDD-Sim)

[![CI](https://github.com/Pratzz1202/market-data-dissemination-simulator/actions/workflows/ci.yml/badge.svg)](https://github.com/Pratzz1202/market-data-dissemination-simulator/actions/workflows/ci.yml)

MDD-Sim is a market data distribution system built on C++20 and gRPC
bidirectional streaming. It simulates multi-instrument L2 order books,
publishes snapshot + sequenced incrementals, enforces client reconstruction
correctness with gap detection/resync, handles backpressure with bounded
per-instrument queues, and provides deterministic record/replay plus a load
tester.

## Guarantees

- Per-instrument sequence correctness (`seq`, `prev_seq`, `snapshot_seq`).
- Deterministic simulation for a fixed `seed` + config.
- Snapshots carry the **full book** at `snapshot_seq`, so snapshot +
  incrementals reconstructs the server book exactly (verified by test).
- Client consistency rules:
  - Apply an incremental only when `prev_seq == last_seq` and `seq > last_seq`.
  - Any mismatch triggers a resync request; duplicates are never applied.
- Backpressure safety:
  - Bounded per-client, per-instrument outbound queues for incrementals.
  - On overflow: drop the incoming incremental, mark the queue dirty, and
    replace it with a reset snapshot on the next publish cycle.
  - Control messages (errors, pongs, unsubscribe confirmations) are never
    dropped by the bound.
- Replayability:
  - Recordings begin with a baseline snapshot per instrument and capture
    generated truth (loss simulation affects publishing only), so
    `mdd_replay --strict` passes on any fresh recording.
  - `mdd_replay` verifies sequence continuity, applies every event, and
    validates final book integrity.

See [docs/design.md](docs/design.md), [docs/protocol.md](docs/protocol.md),
and [docs/benchmarks.md](docs/benchmarks.md).

## Layout

```
mdd-sim/
  proto/mdd.proto        protocol definition (single bidi Stream RPC)
  server/                simulator, order book, subscriptions, publisher, service
  client/                session (reconnect/resubscribe), apply engine, book view
  common/                logging, metrics, config loading, record/replay IO
  tools/                 loadtest.cpp, replay.cpp
  tests/                 unit + integration tests (ctest)
  docker/                Dockerfiles + compose
  docs/                  design, protocol, benchmarks, bench configs
  instruments.json       example instrument config
```

## Build

Requirements: CMake 3.20+, a C++20 compiler, Protobuf, gRPC.

```bash
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

The default build type is `Release`. On macOS, install dependencies with
`brew install cmake grpc protobuf`; on Ubuntu with
`apt-get install cmake protobuf-compiler protobuf-compiler-grpc libprotobuf-dev libgrpc++-dev`.

Sanitizers:

```bash
cmake -S . -B build-asan -DMDD_SIM_ENABLE_ASAN=ON -DMDD_SIM_ENABLE_UBSAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

## Run

Server:

```bash
./build/mdd_server --config instruments.json --seed 123 \
  --address 0.0.0.0:50051 --health_port 8081
```

Health endpoints on `--health_port`: `/healthz`, `/readyz`, `/metrics`
(Prometheus text; counters use the `mdd_` prefix). `--log_level debug`
enables per-update logs; the default `info` level keeps the publish path
free of logging cost.

Client:

```bash
./build/mdd_client --host localhost:50051 --subscribe BTC-USD --subscribe ETH-USD --depth 10
```

Interactive mode (`sub`, `unsub`, `ping`, `show`, `help`, `quit`):

```bash
./build/mdd_client --host localhost:50051 --interactive
```

Load test (in-process, no separate server needed):

```bash
./build/mdd_loadtest --inprocess_config docs/bench_configs/fanout_1x100_1k.json \
  --clients 100 --duration_sec 8
```

`--warmup_sec` (default 1) excludes startup churn from all measurements;
`--synthetic_instruments N` replicates the first configured instrument N
times for multi-instrument scenarios. See
[docs/benchmarks.md](docs/benchmarks.md) for scenarios and current numbers.

Record and replay:

```bash
./build/mdd_server --config instruments.json --record_path /tmp/mdd.record
# ... stop with SIGINT/SIGTERM ...
./build/mdd_replay --record_path /tmp/mdd.record --strict
```

`--strict` exits non-zero on any sequence gap, apply failure, or invalid
final book.

## Failure simulation

- `--drop_every_n N` (server or loadtest): drop every Nth generated
  incremental before publish. Clients gap-detect on the next incremental and
  recover via resync snapshot. Recordings are unaffected by design.
- `--incremental_processing_delay_ms` (loadtest): simulate slow consumers to
  exercise the backpressure drop + reset-snapshot path.

## Quality gates

- `ctest` suite: order book, apply engine, publisher/backpressure,
  subscription manager, config validation, gRPC integration,
  client-book-equals-server-book consistency, record/replay determinism.
- CI (GitHub Actions): Linux build + tests + enforced clang-format and
  clang-tidy (`WarningsAsErrors: '*'`), macOS build + tests, ASan/UBSan
  smoke, benchmark smoke, record/replay strict smoke.
