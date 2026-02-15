#include "common/metrics.h"

#include <sstream>

#include "common/logging.h"

namespace mdd::common {

void MetricsRegistry::SetConnectedClients(uint64_t v) { connected_clients_.store(v); }

void MetricsRegistry::IncrementSnapshots(uint64_t v) { total_snapshots_.fetch_add(v); }

void MetricsRegistry::IncrementIncrementals(uint64_t v) {
  total_incrementals_.fetch_add(v);

  const uint64_t now_ns = NowNs();
  uint64_t start_ns = rate_window_start_ns_.load();
  if (start_ns == 0) {
    const bool won = rate_window_start_ns_.compare_exchange_strong(start_ns, now_ns);
    if (won) {
      start_ns = now_ns;
    } else {
      start_ns = rate_window_start_ns_.load();
    }
  }

  rate_window_count_.fetch_add(v);
  start_ns = rate_window_start_ns_.load();
  if (start_ns == 0 || now_ns <= start_ns) {
    return;
  }

  const uint64_t elapsed_ns = now_ns - start_ns;
  if (elapsed_ns >= 1000000000ull) {
    const uint64_t count = rate_window_count_.exchange(0);
    rate_window_start_ns_.store(now_ns);
    const uint64_t rate = (count * 1000000000ull) / elapsed_ns;
    incremental_rate_per_sec_.store(rate);
  }
}

void MetricsRegistry::IncrementResyncs(uint64_t v) { total_resyncs_.fetch_add(v); }

void MetricsRegistry::IncrementBackpressureDrops(uint64_t v) {
  total_backpressure_drops_.fetch_add(v);
}

void MetricsRegistry::IncrementLossSimulatedDrops(uint64_t v) {
  total_loss_simulated_drops_.fetch_add(v);
}

void MetricsRegistry::IncrementDrops(uint64_t v) { IncrementBackpressureDrops(v); }

void MetricsRegistry::SetClientLag(const std::string& client_id, uint64_t lag) {
  std::lock_guard<std::mutex> lock(lag_mu_);
  client_lag_[client_id] = lag;
}

void MetricsRegistry::RemoveClientLag(const std::string& client_id) {
  std::lock_guard<std::mutex> lock(lag_mu_);
  client_lag_.erase(client_id);
}

MetricsSnapshot MetricsRegistry::Snapshot() const {
  MetricsSnapshot snapshot;
  snapshot.connected_clients = connected_clients_.load();
  snapshot.total_snapshots = total_snapshots_.load();
  snapshot.total_incrementals = total_incrementals_.load();
  snapshot.incremental_rate_per_sec = incremental_rate_per_sec_.load();
  snapshot.total_resyncs = total_resyncs_.load();
  snapshot.total_backpressure_drops = total_backpressure_drops_.load();
  snapshot.total_loss_simulated_drops = total_loss_simulated_drops_.load();
  snapshot.total_drops = snapshot.total_backpressure_drops + snapshot.total_loss_simulated_drops;
  return snapshot;
}

std::string MetricsRegistry::ToPrometheusText() const {
  const auto s = Snapshot();
  std::unordered_map<std::string, uint64_t> lag_copy;
  {
    std::lock_guard<std::mutex> lock(lag_mu_);
    lag_copy = client_lag_;
  }

  std::ostringstream oss;
  oss << "# TYPE connected_clients gauge\n";
  oss << "connected_clients " << s.connected_clients << "\n";
  oss << "# TYPE total_snapshots counter\n";
  oss << "total_snapshots " << s.total_snapshots << "\n";
  oss << "# TYPE total_incrementals counter\n";
  oss << "total_incrementals " << s.total_incrementals << "\n";
  oss << "# TYPE incremental_rate_per_sec gauge\n";
  oss << "incremental_rate_per_sec " << s.incremental_rate_per_sec << "\n";
  oss << "# TYPE total_resyncs counter\n";
  oss << "total_resyncs " << s.total_resyncs << "\n";
  oss << "# TYPE total_backpressure_drops counter\n";
  oss << "total_backpressure_drops " << s.total_backpressure_drops << "\n";
  oss << "# TYPE total_loss_simulated_drops counter\n";
  oss << "total_loss_simulated_drops " << s.total_loss_simulated_drops << "\n";
  oss << "# TYPE total_drops counter\n";
  oss << "total_drops " << s.total_drops << "\n";
  oss << "# TYPE client_lag gauge\n";
  for (const auto& [client_id, lag] : lag_copy) {
    oss << "client_lag{client_id=\"" << client_id << "\"} " << lag << "\n";
  }
  return oss.str();
}

MetricsRegistry& GlobalMetrics() {
  static MetricsRegistry registry;
  return registry;
}

}  // namespace mdd::common
