#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace mdd::common {

struct MetricsSnapshot {
  uint64_t connected_clients = 0;
  uint64_t total_snapshots = 0;
  uint64_t total_incrementals = 0;
  uint64_t incremental_rate_per_sec = 0;
  uint64_t total_resyncs = 0;
  uint64_t total_backpressure_drops = 0;
  uint64_t total_loss_simulated_drops = 0;
  uint64_t total_drops = 0;
};

class MetricsRegistry {
 public:
  using ClientLagProvider = std::function<std::vector<std::pair<std::string, uint64_t>>()>;

  MetricsRegistry();

  void SetConnectedClients(uint64_t v);
  void IncrementSnapshots(uint64_t v = 1);
  void IncrementIncrementals(uint64_t v = 1);
  void IncrementResyncs(uint64_t v = 1);
  void IncrementBackpressureDrops(uint64_t v = 1);
  void IncrementLossSimulatedDrops(uint64_t v = 1);

  // Client lag is sampled through this provider at scrape time only, so the
  // publish hot path never touches registry state beyond atomic counters.
  // Pass nullptr to detach (the provider's owner must do so before dying).
  void SetClientLagProvider(ClientLagProvider provider);

  // The incremental rate is derived from counter deltas between Snapshot()
  // calls (windows shorter than 100ms reuse the previous value). The first
  // call reports the average rate since registry construction.
  MetricsSnapshot Snapshot() const;
  std::string ToPrometheusText() const;

 private:
  std::atomic<uint64_t> connected_clients_{0};
  std::atomic<uint64_t> total_snapshots_{0};
  std::atomic<uint64_t> total_incrementals_{0};
  std::atomic<uint64_t> total_resyncs_{0};
  std::atomic<uint64_t> total_backpressure_drops_{0};
  std::atomic<uint64_t> total_loss_simulated_drops_{0};

  mutable std::mutex rate_mu_;
  mutable uint64_t rate_window_start_ns_ = 0;
  mutable uint64_t rate_window_total_ = 0;
  mutable uint64_t last_rate_per_sec_ = 0;

  mutable std::mutex provider_mu_;
  ClientLagProvider client_lag_provider_;
};

MetricsRegistry& GlobalMetrics();

}  // namespace mdd::common
