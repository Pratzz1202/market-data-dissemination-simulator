#include <grpcpp/grpcpp.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "client/client_session.h"
#include "common/config_loader.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "common/parse.h"
#include "server/instrument_config.h"
#include "server/market_data_service.h"

namespace {

struct CliOptions {
  std::string host = "localhost:50051";
  std::string server_metrics_endpoint;
  uint32_t clients = 10;
  uint32_t duration_sec = 30;
  uint32_t warmup_sec = 1;
  uint32_t depth = 10;
  uint32_t incremental_processing_delay_ms = 0;
  std::vector<std::string> instruments;

  std::string inprocess_config;
  uint32_t synthetic_instruments = 0;
  uint64_t seed = 123;
  uint32_t drop_every_n = 0;
  size_t queue_limit_per_instrument = 256;

  bool verbose_logs = false;
  bool help = false;
};

struct InProcessHarness {
  bool Start(const CliOptions& options, std::vector<std::string>* instruments, std::string* error) {
    mdd::InstrumentsConfig config_proto;
    std::string config_error;
    if (!mdd::common::LoadConfigFromJson(options.inprocess_config, &config_proto, &config_error)) {
      if (error != nullptr) {
        *error = "inprocess config error: " + config_error;
      }
      return false;
    }

    mdd::server::RuntimeConfig runtime_config = mdd::server::BuildRuntimeConfig(config_proto);

    // Multi-instrument scenarios replicate the first configured instrument
    // instead of requiring hundreds of copy-pasted JSON blocks.
    if (options.synthetic_instruments > 0) {
      const mdd::server::InstrumentRuntimeConfig prototype = runtime_config.instruments.front();
      runtime_config.instruments.clear();
      runtime_config.instruments.reserve(options.synthetic_instruments);
      for (uint32_t i = 0; i < options.synthetic_instruments; ++i) {
        mdd::server::InstrumentRuntimeConfig instrument = prototype;
        char suffix[16];
        std::snprintf(suffix, sizeof(suffix), "-%03u", i + 1);
        instrument.instrument_id = prototype.instrument_id + suffix;
        instrument.symbol = prototype.symbol + suffix;
        instrument.base_price = prototype.base_price + static_cast<int64_t>(i) * 100;
        runtime_config.instruments.push_back(std::move(instrument));
      }
    }

    if (instruments != nullptr && instruments->empty()) {
      for (const auto& instrument : runtime_config.instruments) {
        instruments->push_back(instrument.instrument_id);
      }
    }

    mdd::server::ServiceOptions service_options;
    service_options.server_build_id = "mdd_loadtest_inprocess";
    service_options.queue_limit_per_instrument = options.queue_limit_per_instrument;
    service_options.drop_every_n = options.drop_every_n;

    service = std::make_unique<mdd::server::MarketDataServiceImpl>(runtime_config, options.seed,
                                                                   service_options);

    grpc::ServerBuilder builder;
    builder.RegisterService(service.get());
    server = builder.BuildAndStart();
    if (!server) {
      if (error != nullptr) {
        *error = "failed to start in-process server";
      }
      return false;
    }

    service->StartSimulation();

    grpc::ChannelArguments args;
    channel = server->InProcessChannel(args);
    if (!channel) {
      if (error != nullptr) {
        *error = "failed to create in-process channel";
      }
      return false;
    }

    started = true;
    return true;
  }

  void Stop() {
    if (!started) {
      return;
    }
    if (service != nullptr) {
      service->StopSimulation();
    }
    if (server != nullptr) {
      server->Shutdown();
    }
    started = false;
  }

  ~InProcessHarness() { Stop(); }

  std::unique_ptr<mdd::server::MarketDataServiceImpl> service;
  std::unique_ptr<grpc::Server> server;
  std::shared_ptr<grpc::Channel> channel;
  bool started = false;
};

bool ParseArgs(int argc, char** argv, CliOptions* options, std::string* error) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto next = [&]() -> const char* {
      if (i + 1 >= argc) {
        if (error != nullptr) {
          *error = "missing value for " + arg;
        }
        return nullptr;
      }
      return argv[++i];
    };
    auto next_number = [&](auto* out) {
      const char* value = next();
      if (value == nullptr) {
        return false;
      }
      if (!mdd::common::ParseNumber(value, out)) {
        if (error != nullptr) {
          *error = "invalid numeric value for " + arg + ": " + value;
        }
        return false;
      }
      return true;
    };

    if (arg == "--host") {
      const char* value = next();
      if (value == nullptr) return false;
      options->host = value;
    } else if (arg == "--server_metrics_endpoint") {
      const char* value = next();
      if (value == nullptr) return false;
      options->server_metrics_endpoint = value;
    } else if (arg == "--clients") {
      if (!next_number(&options->clients)) return false;
    } else if (arg == "--duration_sec") {
      if (!next_number(&options->duration_sec)) return false;
    } else if (arg == "--warmup_sec") {
      if (!next_number(&options->warmup_sec)) return false;
    } else if (arg == "--depth") {
      if (!next_number(&options->depth)) return false;
    } else if (arg == "--incremental_processing_delay_ms") {
      if (!next_number(&options->incremental_processing_delay_ms)) return false;
    } else if (arg == "--instrument") {
      const char* value = next();
      if (value == nullptr) return false;
      options->instruments.emplace_back(value);
    } else if (arg == "--inprocess_config") {
      const char* value = next();
      if (value == nullptr) return false;
      options->inprocess_config = value;
    } else if (arg == "--synthetic_instruments") {
      if (!next_number(&options->synthetic_instruments)) return false;
    } else if (arg == "--seed") {
      if (!next_number(&options->seed)) return false;
    } else if (arg == "--drop_every_n") {
      if (!next_number(&options->drop_every_n)) return false;
    } else if (arg == "--queue_limit_per_instrument") {
      if (!next_number(&options->queue_limit_per_instrument)) return false;
    } else if (arg == "--verbose_logs") {
      options->verbose_logs = true;
    } else if (arg == "--help") {
      options->help = true;
      return true;
    } else {
      if (error != nullptr) {
        *error = "unknown arg: " + arg;
      }
      return false;
    }
  }

  if (options->duration_sec == 0) {
    if (error != nullptr) {
      *error = "--duration_sec must be > 0";
    }
    return false;
  }

  if (options->synthetic_instruments > 0 && options->inprocess_config.empty()) {
    if (error != nullptr) {
      *error = "--synthetic_instruments requires --inprocess_config";
    }
    return false;
  }

  if (options->inprocess_config.empty() && options->instruments.empty()) {
    if (error != nullptr) {
      *error = "at least one --instrument is required when not using --inprocess_config";
    }
    return false;
  }

  return true;
}

void PrintUsage() {
  std::cerr << "Usage: mdd_loadtest [--host <host:port> | --inprocess_config <path>] "
            << "[options]\n"
            << "  --instrument <id>   repeatable, optional with --inprocess_config\n"
            << "  --synthetic_instruments <n>   replicate first config instrument n times\n"
            << "  --server_metrics_endpoint <http://host:port/metrics>\n"
            << "  --clients <n>\n"
            << "  --duration_sec <n>   measurement window (after warmup)\n"
            << "  --warmup_sec <n>     excluded from all measurements (default 1)\n"
            << "  --depth <n>\n"
            << "  --incremental_processing_delay_ms <ms>\n"
            << "  --seed <n>\n"
            << "  --drop_every_n <n>\n"
            << "  --queue_limit_per_instrument <n>\n"
            << "  --verbose_logs\n"
            << "  --help\n";
}

struct HttpEndpoint {
  std::string host;
  std::string port;
  std::string path;
};

bool ParseHttpEndpoint(const std::string& endpoint, HttpEndpoint* parsed, std::string* error) {
  static constexpr const char* kPrefix = "http://";
  if (!endpoint.starts_with(kPrefix)) {
    if (error != nullptr) {
      *error = "server_metrics_endpoint must start with http://";
    }
    return false;
  }

  const std::string rest = endpoint.substr(std::char_traits<char>::length(kPrefix));
  const size_t slash_pos = rest.find('/');
  const std::string host_port = slash_pos == std::string::npos ? rest : rest.substr(0, slash_pos);
  const std::string path = slash_pos == std::string::npos ? "/metrics" : rest.substr(slash_pos);
  const size_t colon_pos = host_port.rfind(':');
  if (host_port.empty() || colon_pos == std::string::npos || colon_pos == 0 ||
      colon_pos + 1 >= host_port.size()) {
    if (error != nullptr) {
      *error = "server_metrics_endpoint must be formatted as http://host:port/path";
    }
    return false;
  }

  if (parsed != nullptr) {
    parsed->host = host_port.substr(0, colon_pos);
    parsed->port = host_port.substr(colon_pos + 1);
    parsed->path = path;
  }
  return true;
}

bool FetchHttpBody(const HttpEndpoint& endpoint, std::string* body, std::string* error) {
  if (body == nullptr) {
    if (error != nullptr) {
      *error = "internal error: null body pointer";
    }
    return false;
  }

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo* results = nullptr;
  const int rc = ::getaddrinfo(endpoint.host.c_str(), endpoint.port.c_str(), &hints, &results);
  if (rc != 0) {
    if (error != nullptr) {
      *error = std::string("getaddrinfo failed: ") + ::gai_strerror(rc);
    }
    return false;
  }

  int socket_fd = -1;
  for (addrinfo* ai = results; ai != nullptr; ai = ai->ai_next) {
    socket_fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (socket_fd < 0) {
      continue;
    }
    if (::connect(socket_fd, ai->ai_addr, ai->ai_addrlen) == 0) {
      break;
    }
    ::close(socket_fd);
    socket_fd = -1;
  }
  ::freeaddrinfo(results);

  if (socket_fd < 0) {
    if (error != nullptr) {
      *error = "failed to connect to metrics endpoint";
    }
    return false;
  }

  const std::string request = "GET " + endpoint.path + " HTTP/1.1\r\nHost: " + endpoint.host +
                              "\r\nConnection: close\r\n\r\n";

  size_t sent = 0;
  while (sent < request.size()) {
    const ssize_t n = ::send(socket_fd, request.data() + sent, request.size() - sent, 0);
    if (n <= 0) {
      ::close(socket_fd);
      if (error != nullptr) {
        *error = "failed to send metrics request";
      }
      return false;
    }
    sent += static_cast<size_t>(n);
  }

  std::string response;
  char buffer[4096];
  while (true) {
    const ssize_t n = ::recv(socket_fd, buffer, sizeof(buffer), 0);
    if (n <= 0) {
      break;
    }
    response.append(buffer, static_cast<size_t>(n));
  }
  ::close(socket_fd);

  if (response.empty()) {
    if (error != nullptr) {
      *error = "empty response from metrics endpoint";
    }
    return false;
  }

  if (!response.starts_with("HTTP/1.1 200") && !response.starts_with("HTTP/1.0 200")) {
    if (error != nullptr) {
      *error = "metrics endpoint returned non-200 response";
    }
    return false;
  }

  const size_t header_end = response.find("\r\n\r\n");
  if (header_end == std::string::npos) {
    if (error != nullptr) {
      *error = "invalid HTTP response";
    }
    return false;
  }

  *body = response.substr(header_end + 4);
  return true;
}

std::optional<uint64_t> ParsePrometheusValue(const std::string& metrics_text,
                                             const std::string& metric_name) {
  const std::string prefix = metric_name + " ";
  size_t start = 0;
  while (start < metrics_text.size()) {
    size_t end = metrics_text.find('\n', start);
    if (end == std::string::npos) {
      end = metrics_text.size();
    }
    const std::string line = metrics_text.substr(start, end - start);
    if (line.rfind(prefix, 0) == 0) {
      errno = 0;
      const char* value_str = line.c_str() + prefix.size();
      char* value_end = nullptr;
      const unsigned long long parsed = std::strtoull(value_str, &value_end, 10);
      if (errno == 0 && value_end != value_str) {
        return static_cast<uint64_t>(parsed);
      }
    }
    start = end + 1;
  }
  return std::nullopt;
}

std::optional<mdd::common::MetricsSnapshot> FetchMetricsSnapshotFromEndpoint(
    const std::string& endpoint, std::string* error) {
  HttpEndpoint parsed_endpoint;
  if (!ParseHttpEndpoint(endpoint, &parsed_endpoint, error)) {
    return std::nullopt;
  }

  std::string metrics_text;
  if (!FetchHttpBody(parsed_endpoint, &metrics_text, error)) {
    return std::nullopt;
  }

  mdd::common::MetricsSnapshot snapshot;
  if (const auto v = ParsePrometheusValue(metrics_text, "mdd_connected_clients")) {
    snapshot.connected_clients = *v;
  }
  if (const auto v = ParsePrometheusValue(metrics_text, "mdd_snapshots_total")) {
    snapshot.total_snapshots = *v;
  }
  if (const auto v = ParsePrometheusValue(metrics_text, "mdd_incrementals_total")) {
    snapshot.total_incrementals = *v;
  }
  if (const auto v = ParsePrometheusValue(metrics_text, "mdd_incremental_rate_per_sec")) {
    snapshot.incremental_rate_per_sec = *v;
  }
  if (const auto v = ParsePrometheusValue(metrics_text, "mdd_resyncs_total")) {
    snapshot.total_resyncs = *v;
  }
  if (const auto v = ParsePrometheusValue(metrics_text, "mdd_backpressure_drops_total")) {
    snapshot.total_backpressure_drops = *v;
  }
  if (const auto v = ParsePrometheusValue(metrics_text, "mdd_loss_simulated_drops_total")) {
    snapshot.total_loss_simulated_drops = *v;
  }
  if (const auto v = ParsePrometheusValue(metrics_text, "mdd_drops_total")) {
    snapshot.total_drops = *v;
  } else {
    snapshot.total_drops = snapshot.total_backpressure_drops + snapshot.total_loss_simulated_drops;
  }

  return snapshot;
}

struct LatencySummary {
  uint64_t p50 = 0;
  uint64_t p99 = 0;
  uint64_t p999 = 0;
  size_t samples = 0;
};

// Sorts in place; percentiles use the nearest-rank method on (n - 1).
LatencySummary Summarize(std::vector<uint64_t>* values) {
  LatencySummary summary;
  summary.samples = values->size();
  if (values->empty()) {
    return summary;
  }
  std::sort(values->begin(), values->end());
  const auto at = [&](double pct) {
    return (*values)[static_cast<size_t>(pct * static_cast<double>(values->size() - 1))];
  };
  summary.p50 = at(0.50);
  summary.p99 = at(0.99);
  summary.p999 = at(0.999);
  return summary;
}

void UpdatePeak(std::atomic<uint64_t>* peak, uint64_t value) {
  uint64_t current = peak->load();
  while (value > current && !peak->compare_exchange_weak(current, value)) {
  }
}

// Written by exactly one session read thread while running; main reads only
// after Stop() joins that thread, so no lock is needed and the measurement
// path never contends across clients.
struct SessionStats {
  std::vector<uint64_t> incremental_latencies_ns;
  std::vector<uint64_t> snapshot_latencies_ns;
};

}  // namespace

int main(int argc, char** argv) {
  CliOptions options;
  std::string parse_error;
  if (!ParseArgs(argc, argv, &options, &parse_error)) {
    if (!parse_error.empty()) {
      std::cerr << "error: " << parse_error << "\n";
    }
    PrintUsage();
    return 1;
  }
  if (options.help) {
    PrintUsage();
    return 0;
  }

  std::ofstream null_log;
  if (!options.verbose_logs) {
    null_log.open("/dev/null");
    mdd::common::Logger::Instance().SetOutput(&null_log);
  } else {
    mdd::common::Logger::Instance().SetMinLevel(mdd::common::LogLevel::kDebug);
  }

  InProcessHarness inprocess;
  if (!options.inprocess_config.empty()) {
    std::string inprocess_error;
    if (!inprocess.Start(options, &options.instruments, &inprocess_error)) {
      std::cerr << "error: " << inprocess_error << "\n";
      return 1;
    }
  }

  if (options.instruments.empty()) {
    std::cerr << "error: no instruments available to subscribe\n";
    return 1;
  }

  std::atomic<bool> collecting{false};
  std::atomic<uint64_t> incrementals{0};
  std::atomic<uint64_t> resyncs{0};
  std::atomic<uint64_t> errors{0};
  std::atomic<uint64_t> connected{0};
  std::atomic<uint64_t> peak_connected{0};

  std::vector<SessionStats> session_stats(options.clients);
  std::vector<std::unique_ptr<mdd::client::ClientSession>> sessions;
  sessions.reserve(options.clients);

  for (uint32_t i = 0; i < options.clients; ++i) {
    mdd::client::ClientSessionOptions client_options;
    client_options.target = options.host;
    client_options.auto_reconnect = true;
    client_options.reconnect_delay_ms = 500;
    client_options.verbose = options.verbose_logs;
    client_options.incremental_processing_delay_ms = options.incremental_processing_delay_ms;
    if (inprocess.channel != nullptr) {
      client_options.channel_override = inprocess.channel;
      client_options.target = "inprocess";
    }

    auto session = std::make_unique<mdd::client::ClientSession>(client_options);

    SessionStats* stats = &session_stats[i];
    mdd::client::ClientSessionCallbacks callbacks;
    callbacks.on_incremental = [&] {
      if (collecting.load(std::memory_order_relaxed)) {
        incrementals.fetch_add(1, std::memory_order_relaxed);
      }
    };
    callbacks.on_resync_requested = [&](const std::string&) {
      if (collecting.load(std::memory_order_relaxed)) {
        resyncs.fetch_add(1, std::memory_order_relaxed);
      }
    };
    callbacks.on_error = [&](const std::string&) {
      if (collecting.load(std::memory_order_relaxed)) {
        errors.fetch_add(1, std::memory_order_relaxed);
      }
    };
    callbacks.on_connected = [&] {
      const uint64_t current = connected.fetch_add(1) + 1;
      UpdatePeak(&peak_connected, current);
    };
    callbacks.on_disconnected = [&] { connected.fetch_sub(1); };
    callbacks.on_incremental_latency_ns = [&collecting, stats](uint64_t ns) {
      if (collecting.load(std::memory_order_relaxed)) {
        stats->incremental_latencies_ns.push_back(ns);
      }
    };
    callbacks.on_snapshot_latency_ns = [&collecting, stats](uint64_t ns) {
      if (collecting.load(std::memory_order_relaxed)) {
        stats->snapshot_latencies_ns.push_back(ns);
      }
    };
    session->SetCallbacks(std::move(callbacks));

    session->Start();
    for (const auto& instrument : options.instruments) {
      session->Subscribe(instrument, options.depth);
    }

    sessions.push_back(std::move(session));
  }

  if (options.warmup_sec > 0) {
    std::this_thread::sleep_for(std::chrono::seconds(options.warmup_sec));
  }

  // Baseline for server-side counters so warmup churn (connection storms,
  // initial snapshots) is excluded from the reported deltas.
  std::optional<mdd::common::MetricsSnapshot> baseline;
  if (!options.inprocess_config.empty()) {
    baseline = mdd::common::GlobalMetrics().Snapshot();
  } else if (!options.server_metrics_endpoint.empty()) {
    std::string metrics_error;
    baseline = FetchMetricsSnapshotFromEndpoint(options.server_metrics_endpoint, &metrics_error);
    if (!baseline.has_value()) {
      std::cerr << "warn: baseline metrics fetch failed: " << metrics_error << "\n";
    }
  }

  collecting.store(true);
  const auto start = std::chrono::steady_clock::now();
  std::this_thread::sleep_for(std::chrono::seconds(options.duration_sec));
  const auto end = std::chrono::steady_clock::now();
  collecting.store(false);

  std::optional<mdd::common::MetricsSnapshot> final_metrics;
  if (!options.inprocess_config.empty()) {
    final_metrics = mdd::common::GlobalMetrics().Snapshot();
  } else if (!options.server_metrics_endpoint.empty()) {
    std::string metrics_error;
    final_metrics =
        FetchMetricsSnapshotFromEndpoint(options.server_metrics_endpoint, &metrics_error);
    if (!final_metrics.has_value()) {
      std::cerr << "warn: failed to fetch server metrics from " << options.server_metrics_endpoint
                << ": " << metrics_error << "\n";
    }
  }

  for (auto& session : sessions) {
    session->Stop();
  }
  inprocess.Stop();

  const double elapsed_sec = std::chrono::duration<double>(end - start).count();
  const uint64_t total_incrementals = incrementals.load();
  const double throughput =
      elapsed_sec > 0.0 ? static_cast<double>(total_incrementals) / elapsed_sec : 0.0;

  std::vector<uint64_t> incremental_latencies;
  std::vector<uint64_t> snapshot_latencies;
  for (auto& stats : session_stats) {
    incremental_latencies.insert(incremental_latencies.end(),
                                 stats.incremental_latencies_ns.begin(),
                                 stats.incremental_latencies_ns.end());
    snapshot_latencies.insert(snapshot_latencies.end(), stats.snapshot_latencies_ns.begin(),
                              stats.snapshot_latencies_ns.end());
  }
  const LatencySummary inc = Summarize(&incremental_latencies);
  const LatencySummary snap = Summarize(&snapshot_latencies);

  std::cout << "=== mdd_loadtest summary ===\n";
  std::cout << "mode=" << (options.inprocess_config.empty() ? "remote" : "inprocess")
            << " host=" << options.host << " clients=" << options.clients
            << " instruments=" << options.instruments.size() << " depth=" << options.depth
            << " warmup_sec=" << options.warmup_sec << " duration_sec=" << options.duration_sec
            << " incremental_processing_delay_ms=" << options.incremental_processing_delay_ms
            << "\n";
  std::cout << "delivered_incrementals_per_sec=" << throughput
            << "  (sum across all clients; per-client rate = this / clients)\n";
  std::cout << "incremental_latency_ns p50=" << inc.p50 << " p99=" << inc.p99
            << " p999=" << inc.p999 << " samples=" << inc.samples << "\n";
  std::cout << "snapshot_latency_ns p50=" << snap.p50 << " p99=" << snap.p99
            << " p999=" << snap.p999 << " samples=" << snap.samples << "\n";
  std::cout << "client_counters delivered=" << total_incrementals << " resyncs=" << resyncs.load()
            << " errors=" << errors.load() << " peak_connected=" << peak_connected.load() << "\n";

  if (final_metrics.has_value()) {
    // .value() + local copies: cheap (a struct of counters) and keeps
    // bugprone-unchecked-optional-access satisfied across compiler versions.
    const mdd::common::MetricsSnapshot current = final_metrics.value();
    if (baseline.has_value()) {
      const mdd::common::MetricsSnapshot base = baseline.value();
      std::cout << "server_counters_delta backpressure_drops="
                << current.total_backpressure_drops - base.total_backpressure_drops
                << " loss_simulated_drops="
                << current.total_loss_simulated_drops - base.total_loss_simulated_drops
                << " resyncs_served=" << current.total_resyncs - base.total_resyncs
                << " incrementals_enqueued=" << current.total_incrementals - base.total_incrementals
                << "\n";
    } else {
      std::cout << "server_counters_absolute backpressure_drops="
                << current.total_backpressure_drops
                << " loss_simulated_drops=" << current.total_loss_simulated_drops
                << " resyncs_served=" << current.total_resyncs
                << " incrementals_enqueued=" << current.total_incrementals
                << "  (no baseline; includes pre-run history)\n";
    }
    std::cout << "server_incremental_rate_per_sec=" << current.incremental_rate_per_sec << "\n";
  } else {
    std::cout << "server_counters=unavailable (remote mode without "
                 "--server_metrics_endpoint)\n";
  }

  return 0;
}
