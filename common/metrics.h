#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

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
  void SetConnectedClients(uint64_t v);
  void IncrementSnapshots(uint64_t v = 1);
  void IncrementIncrementals(uint64_t v = 1);
  void IncrementResyncs(uint64_t v = 1);
  void IncrementBackpressureDrops(uint64_t v = 1);
  void IncrementLossSimulatedDrops(uint64_t v = 1);
  void IncrementDrops(uint64_t v = 1);  // compatibility alias to backpressure drops
  void SetClientLag(const std::string& client_id, uint64_t lag);
  void RemoveClientLag(const std::string& client_id);

  MetricsSnapshot Snapshot() const;
  std::string ToPrometheusText() const;

 private:
  std::atomic<uint64_t> connected_clients_{0};
  std::atomic<uint64_t> total_snapshots_{0};
  std::atomic<uint64_t> total_incrementals_{0};
  std::atomic<uint64_t> incremental_rate_per_sec_{0};
  std::atomic<uint64_t> rate_window_start_ns_{0};
  std::atomic<uint64_t> rate_window_count_{0};
  std::atomic<uint64_t> total_resyncs_{0};
  std::atomic<uint64_t> total_backpressure_drops_{0};
  std::atomic<uint64_t> total_loss_simulated_drops_{0};
  mutable std::mutex lag_mu_;
  std::unordered_map<std::string, uint64_t> client_lag_;
};

MetricsRegistry& GlobalMetrics();

}  // namespace mdd::common
