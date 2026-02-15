# Market Data Dissemination Simulator (MDD-Sim)

[![CI](https://github.com/pratzz/Market-Data-Dissemination-Simulator/actions/workflows/ci.yml/badge.svg)](https://github.com/pratzz/Market-Data-Dissemination-Simulator/actions/workflows/ci.yml)

MDD-Sim is a production-style market data distribution system using C++ and gRPC bidirectional streaming. It simulates multi-instrument L2 order books, publishes snapshot + sequenced incrementals, enforces client reconstruction correctness with gap detection/resync, handles backpressure with bounded per-instrument queues, and provides deterministic record/replay plus load-test benchmarks.

## Key Guarantees

- Sequence correctness per instrument (`seq`, `prev_seq`, snapshot `snapshot_seq`).
- Deterministic simulation for fixed `seed` + config.
- Snapshot semantics: full published state exactly at `snapshot_seq`.
- Client consistency rules:
  - Apply incremental only when `prev_seq == last_seq`.
  - Any mismatch triggers resync request.
- Backpressure safety:
  - Bounded per-client, per-instrument outbound queues.
  - Drop newest incrementals on overflow.
  - Mark stream dirty and force reset snapshot before resuming.
- Replayability:
  - Binary record file captures snapshots/incrementals.
  - `mdd_replay` validates sequence continuity and final state.

## Protocol Highlights

- Single bidirectional RPC: `Stream(stream ClientMsg) returns (stream ServerMsg)`.
- Client commands: `Hello`, `Subscribe`, `Unsubscribe`, `ResyncRequest`, `Ping`.
- Server events: `ServerHello`, `Snapshot`, `Incremental`, `Unsubscribed`, `Error`, `Pong`.
- Compatibility support:
  - `schema_version`
  - `client_build_id` / `server_build_id`
  - negotiated `capabilities`

See `/Users/pratzz/Desktop/Market Data Dissemination Simulator/mdd-sim/docs/protocol.md` for message semantics.

## Repository Layout

```
mdd-sim/
  README.md
  docs/
    design.md
    protocol.md
    benchmarks.md
  proto/
    mdd.proto
  server/
    main.cpp
    market_data_service.cpp/h
    instrument_config.cpp/h
    order_book.cpp/h
    simulator.cpp/h
    subscription_manager.cpp/h
    publisher.cpp/h
  client/
    main.cpp
    client_session.cpp/h
    apply_engine.cpp/h
    order_book_view.cpp/h
  tools/
    loadtest.cpp
    replay.cpp
  tests/
    test_order_book.cpp
    test_client_apply.cpp
    test_integration.cpp
    test_replay_determinism.cpp
  cmake/
  CMakeLists.txt
  docker/
    Dockerfile.server
    Dockerfile.client
    docker-compose.yml
```

## Build

Requirements:

- CMake 3.20+
- C++20 compiler
- Protobuf
- gRPC
- pthreads

```bash
cd /Users/pratzz/Desktop/Market Data Dissemination Simulator/mdd-sim
cmake -S . -B build
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Sanitizers:

```bash
cmake -S . -B build-asan -DMDD_SIM_ENABLE_ASAN=ON -DMDD_SIM_ENABLE_UBSAN=ON
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

## Run

### 1) Start server

```bash
./build/mdd_server \
  --config /Users/pratzz/Desktop/Market Data Dissemination Simulator/mdd-sim/instruments.json \
  --seed 123 \
  --address 0.0.0.0:50051 \
  --health_port 8081 \
  --drop_every_n 0
```

Health endpoints:

- `/healthz`
- `/readyz`
- `/metrics`

### 2) Start client

```bash
./build/mdd_client \
  --host localhost:50051 \
  --subscribe BTC-USD \
  --subscribe ETH-USD \
  --depth 10
```

Interactive runtime subscription mode:

```bash
./build/mdd_client --host localhost:50051 --interactive
```

Runtime commands:

- `sub <instrument> [depth]`
- `unsub <instrument>`
- `ping`
- `show <instrument>`
- `help`
- `quit`

### 3) Load test

```bash
./build/mdd_loadtest \
  --host localhost:50051 \
  --clients 100 \
  --instrument BTC-USD \
  --duration_sec 30
```

In-process benchmark mode (no external server process required):

```bash
./build/mdd_loadtest \
  --inprocess_config ./docs/bench_configs/fanout_1x100_1k.json \
  --clients 100 \
  --duration_sec 8
```

### 4) Record/replay

Server with recording:

```bash
./build/mdd_server --config instruments.json --record_path /tmp/mdd.record
```

Replay verification:

```bash
./build/mdd_replay --record_path /tmp/mdd.record --strict
```

## Message Loss Simulation Toggle

Use `--drop_every_n N` on server to intentionally drop every Nth generated incremental before publish. This is designed to demonstrate client gap detection and resync recovery paths in an interview/demo scenario.

## Backpressure Policy

Implemented policy A (market-data style):

- Per-client, per-instrument bounded queue.
- If full, drop newest incremental for that instrument.
- Mark client/instrument as dirty.
- Next publish cycle sends reset snapshot and resumes incrementals from consistent sequence state.

## Quality Gates

- Unit + integration tests (`ctest`).
- ASan/UBSan optional build flags.
- Formatting/lint configs included (`.clang-format`, `.clang-tidy`).
- CI workflow under `.github/workflows/ci.yml`.

## Demo Checklist

- `server_start`, `client_connected`, `subscribe`, `snapshot_sent`, `incremental_sent` logs visible.
- Trigger gap by setting `--drop_every_n`.
- Show client `gap_detected` + `resync_requested` + `snapshot_received(is_reset=true)`.
- Run `mdd_loadtest` and capture p50/p99/p999 + throughput.

## Milestone Mapping

- M1: order book core + tests.
- M2: gRPC protocol skeleton + subscribe snapshot.
- M3: sequenced incrementals + client apply engine.
- M4: gap detection + resync path.
- M5: backpressure bounded queue + forced reset.
- M6: load test + latency distribution.
- M7: deterministic record/replay tooling.
