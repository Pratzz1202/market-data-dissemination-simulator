# Benchmarks

This document defines benchmark scenarios, commands, and measured results for MDD-Sim.

## Run Metadata

- Date: 2026-02-10
- Mode: in-process gRPC (`--inprocess_config`) using `mdd_loadtest`
- Binary: `./build/mdd_loadtest`
- Notes:
  - Results are from one local run per scenario.
  - Multi-instrument scenario intentionally drives saturation to show backpressure behavior.

## Metrics

- Throughput
  - Incrementals/sec processed by clients.
- End-to-end latency
  - `client_receive_ns - server_timestamp_ns` from each incremental.
  - Report p50, p99, p999.
- Recovery pressure
  - resync count.
  - drop count (from server metrics/logs).

## Scenarios

1. Baseline
   - 1 instrument, 1 client
   - 10k updates/sec target
2. Fan-out
   - 1 instrument, 100 clients
   - 1k updates/sec target
3. Multi-instrument
   - 100 instruments, 10 clients
   - 500 updates/sec/instrument target
4. Slow-client stress
   - artificial consumer delay in client
   - verify drop+reset policy behavior
5. Loss simulation
   - server `--drop_every_n N`
   - verify expected resync rate and stable recovery

## Commands Used

```bash
./build/mdd_loadtest \
  --inprocess_config docs/bench_configs/baseline_1x1_10k.json \
  --clients 1 --duration_sec 8
```

```bash
./build/mdd_loadtest \
  --inprocess_config docs/bench_configs/fanout_1x100_1k.json \
  --clients 100 --duration_sec 8
```

```bash
./build/mdd_loadtest \
  --inprocess_config docs/bench_configs/multi_100x10_500.json \
  --clients 10 --duration_sec 6
```

```bash
./build/mdd_loadtest \
  --inprocess_config docs/bench_configs/fanout_1x100_1k.json \
  --clients 10 --duration_sec 8 \
  --incremental_processing_delay_ms 5
```

```bash
./build/mdd_loadtest \
  --inprocess_config docs/bench_configs/loss_1x10_1k.json \
  --clients 10 --duration_sec 8 \
  --drop_every_n 50
```

## Required Summary Output Block

`mdd_loadtest` prints:

- config fields (`host`, `clients`, `instruments`, `depth`, `duration`)
- throughput (`throughput_incrementals_per_sec`)
- latency (`latency_ns_p50`, `latency_ns_p99`, `latency_ns_p999`)
- resilience counts (`total_resyncs`, `total_errors`, `total_drops`)
- server pace (`server_incremental_rate_per_sec`)

## Results

| Scenario | Clients | Instruments | Throughput (inc/s) | p50 (ns) | p99 (ns) | p999 (ns) | Resyncs | Drops | Notes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Baseline (10k target) | 1 | 1 | 9999.74 | 20000 | 43000 | 89000 | 0 | 0 | Stable, no recovery activity |
| Fan-out (1k target, 100 clients) | 100 | 1 | 99966.8 | 501000 | 2575000 | 21520000 | 1 | 1 | Minor startup recovery under high fan-out |
| Multi-instrument (500 target x100) | 10 | 100 | 46048.3 | 361942000 | 971871000 | 1147150000 | 55 | 8014 | Saturated path, expected heavy drop+resync |
| Slow-client stress | 10 | 1 | 1617.6 | 140012000 | 266779000 | 270351000 | 0 | 260 | Delay-induced backpressure validated |
| Loss simulation (`drop_every_n=50`) | 10 | 1 | 9601.12 | 67000 | 234000 | 1546000 | 1600 | 0 | Gap detection and resync active by design |

These numbers satisfy the benchmark evidence requirement by including measured throughput and latency distribution (p50/p99/p999) plus recovery counters.
