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
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "client/client_session.h"
#include "common/config_loader.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "server/instrument_config.h"
#include "server/market_data_service.h"

namespace {

struct CliOptions {
  std::string host = "localhost:50051";
  std::string server_metrics_endpoint;
  uint32_t clients = 10;
  uint32_t duration_sec = 30;
  uint32_t depth = 10;
  uint32_t incremental_processing_delay_ms = 0;
  std::vector<std::string> instruments;

  std::string inprocess_config;
  uint64_t seed = 123;
  uint32_t drop_every_n = 0;
  size_t queue_limit_per_instrument = 256;

  bool verbose_logs = false;
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

    const mdd::server::RuntimeConfig runtime_config = mdd::server::BuildRuntimeConfig(config_proto);

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

    if (arg == "--host") {
      const char* value = next();
      if (value == nullptr) return false;
      options->host = value;
    } else if (arg == "--server_metrics_endpoint") {
      const char* value = next();
      if (value == nullptr) return false;
      options->server_metrics_endpoint = value;
    } else if (arg == "--clients") {
      const char* value = next();
      if (value == nullptr) return false;
      options->clients = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--duration_sec") {
      const char* value = next();
      if (value == nullptr) return false;
      options->duration_sec = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--depth") {
      const char* value = next();
      if (value == nullptr) return false;
      options->depth = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--incremental_processing_delay_ms") {
      const char* value = next();
      if (value == nullptr) return false;
      options->incremental_processing_delay_ms = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--instrument") {
      const char* value = next();
      if (value == nullptr) return false;
      options->instruments.push_back(value);
    } else if (arg == "--inprocess_config") {
      const char* value = next();
      if (value == nullptr) return false;
      options->inprocess_config = value;
    } else if (arg == "--seed") {
      const char* value = next();
      if (value == nullptr) return false;
      options->seed = static_cast<uint64_t>(std::stoull(value));
    } else if (arg == "--drop_every_n") {
      const char* value = next();
      if (value == nullptr) return false;
      options->drop_every_n = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--queue_limit_per_instrument") {
      const char* value = next();
      if (value == nullptr) return false;
      options->queue_limit_per_instrument = static_cast<size_t>(std::stoull(value));
    } else if (arg == "--verbose_logs") {
      options->verbose_logs = true;
    } else {
      if (error != nullptr) {
        *error = "unknown arg: " + arg;
      }
      return false;
    }
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
            << "  --server_metrics_endpoint <http://host:port/metrics>\n"
            << "  --clients <n>\n"
            << "  --duration_sec <n>\n"
            << "  --depth <n>\n"
            << "  --incremental_processing_delay_ms <ms>\n"
            << "  --seed <n>\n"
            << "  --drop_every_n <n>\n"
            << "  --queue_limit_per_instrument <n>\n"
            << "  --verbose_logs\n";
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

std::optional<uint64_t> ParsePrometheusCounter(const std::string& metrics_text,
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
  if (const auto v = ParsePrometheusCounter(metrics_text, "connected_clients")) {
    snapshot.connected_clients = *v;
  }
  if (const auto v = ParsePrometheusCounter(metrics_text, "total_snapshots")) {
    snapshot.total_snapshots = *v;
  }
  if (const auto v = ParsePrometheusCounter(metrics_text, "total_incrementals")) {
    snapshot.total_incrementals = *v;
  }
  if (const auto v = ParsePrometheusCounter(metrics_text, "incremental_rate_per_sec")) {
    snapshot.incremental_rate_per_sec = *v;
  }
  if (const auto v = ParsePrometheusCounter(metrics_text, "total_resyncs")) {
    snapshot.total_resyncs = *v;
  }
  if (const auto v = ParsePrometheusCounter(metrics_text, "total_backpressure_drops")) {
    snapshot.total_backpressure_drops = *v;
  }
  if (const auto v = ParsePrometheusCounter(metrics_text, "total_loss_simulated_drops")) {
    snapshot.total_loss_simulated_drops = *v;
  }
  if (const auto v = ParsePrometheusCounter(metrics_text, "total_drops")) {
    snapshot.total_drops = *v;
  } else {
    snapshot.total_drops = snapshot.total_backpressure_drops + snapshot.total_loss_simulated_drops;
  }

  return snapshot;
}

uint64_t Percentile(std::vector<uint64_t> values, double pct) {
  if (values.empty()) {
    return 0;
  }
  std::sort(values.begin(), values.end());
  const size_t idx = static_cast<size_t>(pct * (values.size() - 1));
  return values[idx];
}

void UpdatePeak(std::atomic<uint64_t>* peak, uint64_t value) {
  uint64_t current = peak->load();
  while (value > current && !peak->compare_exchange_weak(current, value)) {
  }
}

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

  std::ofstream null_log;
  if (!options.verbose_logs) {
    null_log.open("/dev/null");
    mdd::common::Logger::Instance().SetOutput(&null_log);
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

  std::atomic<uint64_t> incrementals{0};
  std::atomic<uint64_t> resyncs{0};
  std::atomic<uint64_t> errors{0};
  std::atomic<uint64_t> connected{0};
  std::atomic<uint64_t> peak_connected{0};
  std::mutex incremental_latency_mu;
  std::vector<uint64_t> incremental_latencies_ns;
  std::mutex snapshot_latency_mu;
  std::vector<uint64_t> snapshot_latencies_ns;

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

    mdd::client::ClientSessionCallbacks callbacks;
    callbacks.on_incremental = [&] { incrementals.fetch_add(1); };
    callbacks.on_resync_requested = [&](const std::string&) { resyncs.fetch_add(1); };
    callbacks.on_error = [&](const std::string&) { errors.fetch_add(1); };
    callbacks.on_connected = [&] {
      const uint64_t current = connected.fetch_add(1) + 1;
      UpdatePeak(&peak_connected, current);
    };
    callbacks.on_disconnected = [&] { connected.fetch_sub(1); };
    callbacks.on_incremental_latency_ns = [&](uint64_t ns) {
      std::lock_guard<std::mutex> lock(incremental_latency_mu);
      incremental_latencies_ns.push_back(ns);
    };
    callbacks.on_snapshot_latency_ns = [&](uint64_t ns) {
      std::lock_guard<std::mutex> lock(snapshot_latency_mu);
      snapshot_latencies_ns.push_back(ns);
    };
    session->SetCallbacks(std::move(callbacks));

    session->Start();
    for (const auto& instrument : options.instruments) {
      session->Subscribe(instrument, options.depth);
    }

    sessions.push_back(std::move(session));
  }

  const auto start = std::chrono::steady_clock::now();
  std::this_thread::sleep_for(std::chrono::seconds(options.duration_sec));
  const auto end = std::chrono::steady_clock::now();

  for (auto& session : sessions) {
    session->Stop();
  }

  inprocess.Stop();

  const double elapsed_sec =
      std::chrono::duration_cast<std::chrono::duration<double>>(end - start).count();
  const uint64_t total_incrementals = incrementals.load();
  const double throughput = elapsed_sec > 0.0 ? total_incrementals / elapsed_sec : 0.0;

  std::vector<uint64_t> incremental_latency_copy;
  {
    std::lock_guard<std::mutex> lock(incremental_latency_mu);
    incremental_latency_copy = incremental_latencies_ns;
  }

  std::vector<uint64_t> snapshot_latency_copy;
  {
    std::lock_guard<std::mutex> lock(snapshot_latency_mu);
    snapshot_latency_copy = snapshot_latencies_ns;
  }
  auto SafePercentile = [&](const std::vector<uint64_t>& v, double p) -> uint64_t {
    return v.empty() ? 0ULL : Percentile(v, p);
  };

  const uint64_t inc_p50 =
      incremental_latency_copy.empty() ? 0ULL : Percentile(incremental_latency_copy, 0.50);

  const uint64_t inc_p99 =
      incremental_latency_copy.empty() ? 0ULL : Percentile(incremental_latency_copy, 0.99);

  const uint64_t inc_p999 =
      incremental_latency_copy.empty() ? 0ULL : Percentile(incremental_latency_copy, 0.999);

  const uint64_t snap_p50 =
      snapshot_latency_copy.empty() ? 0ULL : Percentile(snapshot_latency_copy, 0.50);

  const uint64_t snap_p99 =
      snapshot_latency_copy.empty() ? 0ULL : Percentile(snapshot_latency_copy, 0.99);

  const uint64_t snap_p999 =
      snapshot_latency_copy.empty() ? 0ULL : Percentile(snapshot_latency_copy, 0.999);

  auto metrics = mdd::common::GlobalMetrics().Snapshot();
  if (!options.server_metrics_endpoint.empty()) {
    std::string metrics_error;
    const auto remote_metrics =
        FetchMetricsSnapshotFromEndpoint(options.server_metrics_endpoint, &metrics_error);
    if (remote_metrics.has_value()) {
      metrics = *remote_metrics;
    } else {
      std::cerr << "warn: failed to fetch server metrics from " << options.server_metrics_endpoint
                << ": " << metrics_error << "\n";
    }
  }

  std::cout << "=== mdd_loadtest summary ===\n";
  std::cout << "mode=" << (options.inprocess_config.empty() ? "remote" : "inprocess")
            << " host=" << options.host << " clients=" << options.clients
            << " instruments=" << options.instruments.size() << " depth=" << options.depth
            << " duration_sec=" << options.duration_sec
            << " incremental_processing_delay_ms=" << options.incremental_processing_delay_ms
            << " server_metrics_endpoint="
            << (options.server_metrics_endpoint.empty() ? "<local>"
                                                        : options.server_metrics_endpoint)
            << "\n";
  std::cout << "throughput_incrementals_per_sec=" << throughput << "\n";
  std::cout << "incremental_latency_ns_p50=" << inc_p50 << " incremental_latency_ns_p99=" << inc_p99
            << " incremental_latency_ns_p999=" << inc_p999 << "\n";
  std::cout << "snapshot_latency_ns_p50=" << snap_p50 << " snapshot_latency_ns_p99=" << snap_p99
            << " snapshot_latency_ns_p999=" << snap_p999 << "\n";
  std::cout << "total_incrementals=" << total_incrementals << " total_resyncs=" << resyncs.load()
            << " total_errors=" << errors.load()
            << " total_backpressure_drops=" << metrics.total_backpressure_drops
            << " total_loss_simulated_drops=" << metrics.total_loss_simulated_drops
            << " total_drops=" << metrics.total_drops
            << " peak_connected_clients=" << peak_connected.load() << "\n";
  std::cout << "server_incremental_rate_per_sec=" << metrics.incremental_rate_per_sec << "\n";

  return 0;
}
