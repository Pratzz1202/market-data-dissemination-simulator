#include "common/metrics.h"

#include <sstream>

#include "common/logging.h"

namespace mdd::common {

MetricsRegistry::MetricsRegistry() : rate_window_start_ns_(NowNs()) {}

void MetricsRegistry::SetConnectedClients(uint64_t v) { connected_clients_.store(v); }

void MetricsRegistry::IncrementSnapshots(uint64_t v) { total_snapshots_.fetch_add(v); }

void MetricsRegistry::IncrementIncrementals(uint64_t v) { total_incrementals_.fetch_add(v); }

void MetricsRegistry::IncrementResyncs(uint64_t v) { total_resyncs_.fetch_add(v); }

void MetricsRegistry::IncrementBackpressureDrops(uint64_t v) {
  total_backpressure_drops_.fetch_add(v);
}

void MetricsRegistry::IncrementLossSimulatedDrops(uint64_t v) {
  total_loss_simulated_drops_.fetch_add(v);
}

void MetricsRegistry::SetClientLagProvider(ClientLagProvider provider) {
  std::lock_guard<std::mutex> lock(provider_mu_);
  client_lag_provider_ = std::move(provider);
}

MetricsSnapshot MetricsRegistry::Snapshot() const {
  MetricsSnapshot snapshot;
  snapshot.connected_clients = connected_clients_.load();
  snapshot.total_snapshots = total_snapshots_.load();
  snapshot.total_incrementals = total_incrementals_.load();
  snapshot.total_resyncs = total_resyncs_.load();
  snapshot.total_backpressure_drops = total_backpressure_drops_.load();
  snapshot.total_loss_simulated_drops = total_loss_simulated_drops_.load();
  snapshot.total_drops = snapshot.total_backpressure_drops + snapshot.total_loss_simulated_drops;

  {
    std::lock_guard<std::mutex> lock(rate_mu_);
    const uint64_t now_ns = NowNs();
    if (now_ns > rate_window_start_ns_ && snapshot.total_incrementals >= rate_window_total_) {
      const uint64_t elapsed_ns = now_ns - rate_window_start_ns_;
      if (elapsed_ns >= 100000000ull) {
        last_rate_per_sec_ =
            (snapshot.total_incrementals - rate_window_total_) * 1000000000ull / elapsed_ns;
        rate_window_start_ns_ = now_ns;
        rate_window_total_ = snapshot.total_incrementals;
      }
    }
    snapshot.incremental_rate_per_sec = last_rate_per_sec_;
  }

  return snapshot;
}

std::string MetricsRegistry::ToPrometheusText() const {
  const auto s = Snapshot();

  ClientLagProvider provider;
  {
    std::lock_guard<std::mutex> lock(provider_mu_);
    provider = client_lag_provider_;
  }

  std::ostringstream oss;
  oss << "# HELP mdd_connected_clients Currently connected client streams.\n";
  oss << "# TYPE mdd_connected_clients gauge\n";
  oss << "mdd_connected_clients " << s.connected_clients << "\n";
  oss << "# HELP mdd_snapshots_total Snapshots enqueued to clients.\n";
  oss << "# TYPE mdd_snapshots_total counter\n";
  oss << "mdd_snapshots_total " << s.total_snapshots << "\n";
  oss << "# HELP mdd_incrementals_total Incrementals enqueued to clients.\n";
  oss << "# TYPE mdd_incrementals_total counter\n";
  oss << "mdd_incrementals_total " << s.total_incrementals << "\n";
  oss << "# HELP mdd_incremental_rate_per_sec Incremental enqueue rate over the last window.\n";
  oss << "# TYPE mdd_incremental_rate_per_sec gauge\n";
  oss << "mdd_incremental_rate_per_sec " << s.incremental_rate_per_sec << "\n";
  oss << "# HELP mdd_resyncs_total Client resync requests served.\n";
  oss << "# TYPE mdd_resyncs_total counter\n";
  oss << "mdd_resyncs_total " << s.total_resyncs << "\n";
  oss << "# HELP mdd_backpressure_drops_total Incrementals dropped on full client queues.\n";
  oss << "# TYPE mdd_backpressure_drops_total counter\n";
  oss << "mdd_backpressure_drops_total " << s.total_backpressure_drops << "\n";
  oss << "# HELP mdd_loss_simulated_drops_total Incrementals dropped by --drop_every_n.\n";
  oss << "# TYPE mdd_loss_simulated_drops_total counter\n";
  oss << "mdd_loss_simulated_drops_total " << s.total_loss_simulated_drops << "\n";
  oss << "# HELP mdd_drops_total Sum of backpressure and simulated-loss drops.\n";
  oss << "# TYPE mdd_drops_total counter\n";
  oss << "mdd_drops_total " << s.total_drops << "\n";
  oss << "# HELP mdd_client_lag Pending queued messages per client.\n";
  oss << "# TYPE mdd_client_lag gauge\n";
  if (provider) {
    for (const auto& [client_id, lag] : provider()) {
      oss << "mdd_client_lag{client_id=\"" << client_id << "\"} " << lag << "\n";
    }
  }
  return oss.str();
}

MetricsRegistry& GlobalMetrics() {
  static MetricsRegistry registry;
  return registry;
}

}  // namespace mdd::common
