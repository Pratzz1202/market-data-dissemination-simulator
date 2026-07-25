# Benchmarks

Benchmark scenarios, methodology, and measured results for MDD-Sim.

## Environment

- Date: 2026-07-25
- Hardware: Apple M3, 8 cores, 8 GB RAM
- OS: macOS 26.4.1
- Compiler: Apple clang 21.0.0, CMake default `Release` (`-O3`)
- Mode: in-process gRPC (`--inprocess_config`) using `mdd_loadtest`
- One run per scenario (numbers are indicative, not statistically robust)

## Methodology

- A 1 s warmup precedes every measurement window (`--warmup_sec`, default 1).
  Connection setup, initial snapshots, and subscribe churn are excluded from
  every reported number.
- Latency is `client_receive_ns - server_timestamp_ns` per applied
  incremental, sampled into per-session buffers (no shared lock on the
  measurement path). Both timestamps come from the same host clock because the
  benchmark runs in-process; cross-host runs would include clock offset.
- Server counters are reported as deltas across the measurement window.
- Percentiles use the nearest-rank method.
- Hot-path logging is disabled at the log-level check (not merely redirected),
  so formatting costs are absent from the measured path.

Caveats worth knowing before quoting these numbers:

- `delivered_incrementals_per_sec` sums across all clients. Fan-out at 100
  clients x 1k updates/sec is 100k deliveries/sec, not 100k unique updates.
- In-process mode runs every client session (2 threads each), every server
  stream (2 threads each), and all simulator threads in one process. Past
  ~20 threads on 8 cores, tail latencies are dominated by scheduling, which
  is representative of an oversubscribed host, not of network fabric.
- The multi-instrument scenario intentionally targets 500k deliveries/sec
  (100 instruments x 500/s x 10 clients), far past what the host sustains,
  to exercise the backpressure drop + reset-snapshot recovery path.

## Scenarios and commands

```bash
# 1. Baseline: 1 instrument, 1 client, 10k updates/sec
./build/mdd_loadtest --inprocess_config docs/bench_configs/baseline_1x1_10k.json \
  --clients 1 --duration_sec 8

# 2. Fan-out: 1 instrument, 100 clients, 1k updates/sec
./build/mdd_loadtest --inprocess_config docs/bench_configs/fanout_1x100_1k.json \
  --clients 100 --duration_sec 8

# 3. Multi-instrument: 100 synthetic instruments, 10 clients, 500 updates/sec each
./build/mdd_loadtest --inprocess_config docs/bench_configs/multi_template_500.json \
  --synthetic_instruments 100 --clients 10 --duration_sec 6

# 4. Slow-client stress: artificial 5 ms consumer delay
./build/mdd_loadtest --inprocess_config docs/bench_configs/fanout_1x100_1k.json \
  --clients 10 --duration_sec 8 --incremental_processing_delay_ms 5

# 5. Loss simulation: server drops every 50th incremental before publish
./build/mdd_loadtest --inprocess_config docs/bench_configs/fanout_1x100_1k.json \
  --clients 10 --duration_sec 8 --drop_every_n 50
```

## Results

| Scenario | Clients | Instruments | Delivered inc/s | p50 | p99 | p999 | Resyncs | Drops | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Baseline (10k target) | 1 | 1 | 10,000 | 9 µs | 27 µs | 1.9 ms | 0 | 0 | Stable, no recovery activity |
| Fan-out (1k x 100 clients) | 100 | 1 | 99,997 | 488 µs | 4.2 ms | 20 ms | 0 | 0 | Clean startup, no recovery activity |
| Multi-instrument (500 x 100) | 10 | 100 | 129,163 | 267 ms | 503 ms | 513 ms | 0 | 8,633 | Deliberate saturation; reset snapshots recover without client resyncs |
| Slow-client (5 ms delay) | 10 | 1 | 1,631 | 140 ms | 266 ms | 269 ms | 0 | 260 | Drop + reset-snapshot policy handles the lag |
| Loss (`drop_every_n=50`) | 10 | 1 | 9,546 | 138 µs | 2.9 ms | 9.1 ms | 1,600 | 1,600 | Every dropped seq gap-detected and resynced (1600 = 10 clients x 160 drops) |

Reading the recovery columns: in the loss scenario the drop count equals the
client resync count equals the server `resyncs_served` delta — each simulated
loss is detected via `prev_seq` mismatch and repaired with one snapshot. In
the saturation scenarios recovery happens through server-side reset snapshots
on dirty queues instead, which is why client resyncs stay at zero.

## History

Numbers previously published in this file predate the 2026-07 rework and were
measured on an unoptimized build (no `CMAKE_BUILD_TYPE`) with per-update JSON
logging inside the publish path and a global latency-collection lock in the
load tester. They are not comparable and were replaced, not appended to.
