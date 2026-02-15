#include <arpa/inet.h>
#include <grpcpp/grpcpp.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "common/config_loader.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "server/instrument_config.h"
#include "server/market_data_service.h"

namespace {

std::atomic<bool> g_shutdown_requested{false};

void HandleSignal(int /*signal*/) { g_shutdown_requested.store(true); }

struct CliOptions {
  std::string config_path;
  std::string address = "0.0.0.0:50051";
  uint64_t seed = 123;
  size_t queue_limit_per_instrument = 256;
  uint32_t drop_every_n = 0;
  uint16_t health_port = 0;
  std::string record_path;
  std::string server_build_id = "mdd_server_1.0";
};

class HealthHttpServer {
 public:
  HealthHttpServer() = default;
  ~HealthHttpServer() { Stop(); }

  bool Start(uint16_t port, std::string* error) {
    if (running_.exchange(true)) {
      return true;
    }

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
      running_.store(false);
      if (error != nullptr) {
        *error = "failed to create health socket";
      }
      return false;
    }

    const int one = 1;
    (void)::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
      if (error != nullptr) {
        *error = "failed to bind health endpoint on port " + std::to_string(port);
      }
      ::close(listen_fd_);
      listen_fd_ = -1;
      running_.store(false);
      return false;
    }

    if (::listen(listen_fd_, 64) < 0) {
      if (error != nullptr) {
        *error = "failed to listen on health endpoint";
      }
      ::close(listen_fd_);
      listen_fd_ = -1;
      running_.store(false);
      return false;
    }

    thread_ = std::thread([this] { RunLoop(); });
    return true;
  }

  void Stop() {
    if (!running_.exchange(false)) {
      return;
    }
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

 private:
  void WriteResponse(int fd, int status_code, const std::string& status_text,
                     const std::string& body, const std::string& content_type = "text/plain") {
    const std::string response = "HTTP/1.1 " + std::to_string(status_code) + " " + status_text +
                                 "\r\nContent-Type: " + content_type +
                                 "\r\nContent-Length: " + std::to_string(body.size()) +
                                 "\r\nConnection: close\r\n\r\n" + body;
    (void)::send(fd, response.data(), response.size(), 0);
  }

  void HandleRequest(int fd, const std::string& request) {
    if (request.rfind("GET /healthz", 0) == 0 || request.rfind("GET /readyz", 0) == 0) {
      WriteResponse(fd, 200, "OK", "ok\n");
      return;
    }
    if (request.rfind("GET /metrics", 0) == 0) {
      WriteResponse(fd, 200, "OK", mdd::common::GlobalMetrics().ToPrometheusText());
      return;
    }
    WriteResponse(fd, 404, "Not Found", "not found\n");
  }

  void RunLoop() {
    while (running_.load()) {
      sockaddr_in client_addr{};
      socklen_t client_len = sizeof(client_addr);
      const int client_fd =
          ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
      if (client_fd < 0) {
        if (!running_.load()) {
          break;
        }
        continue;
      }

      char buffer[2048];
      const ssize_t received = ::recv(client_fd, buffer, sizeof(buffer) - 1, 0);
      if (received > 0) {
        buffer[received] = '\0';
        HandleRequest(client_fd, std::string(buffer));
      }
      ::close(client_fd);
    }
  }

  std::atomic<bool> running_{false};
  int listen_fd_ = -1;
  std::thread thread_;
};

bool ParseArgs(int argc, char** argv, CliOptions* options, std::string* error) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto require_value = [&](const std::string& flag) -> const char* {
      if (i + 1 >= argc) {
        if (error != nullptr) {
          *error = "missing value for " + flag;
        }
        return nullptr;
      }
      return argv[++i];
    };

    if (arg == "--config") {
      const char* value = require_value(arg);
      if (value == nullptr) return false;
      options->config_path = value;
    } else if (arg == "--address") {
      const char* value = require_value(arg);
      if (value == nullptr) return false;
      options->address = value;
    } else if (arg == "--seed") {
      const char* value = require_value(arg);
      if (value == nullptr) return false;
      options->seed = std::stoull(value);
    } else if (arg == "--queue_limit_per_instrument") {
      const char* value = require_value(arg);
      if (value == nullptr) return false;
      options->queue_limit_per_instrument = static_cast<size_t>(std::stoull(value));
    } else if (arg == "--drop_every_n") {
      const char* value = require_value(arg);
      if (value == nullptr) return false;
      options->drop_every_n = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--health_port") {
      const char* value = require_value(arg);
      if (value == nullptr) return false;
      options->health_port = static_cast<uint16_t>(std::stoul(value));
    } else if (arg == "--record_path") {
      const char* value = require_value(arg);
      if (value == nullptr) return false;
      options->record_path = value;
    } else if (arg == "--server_build_id") {
      const char* value = require_value(arg);
      if (value == nullptr) return false;
      options->server_build_id = value;
    } else if (arg == "--help") {
      return false;
    } else {
      if (error != nullptr) {
        *error = "unknown argument: " + arg;
      }
      return false;
    }
  }

  if (options->config_path.empty()) {
    if (error != nullptr) {
      *error = "--config is required";
    }
    return false;
  }

  return true;
}

void PrintUsage() {
  std::cerr << "Usage: mdd_server --config <instruments.json> [options]\n"
            << "  --address <host:port>\n"
            << "  --seed <uint64>\n"
            << "  --queue_limit_per_instrument <count>\n"
            << "  --drop_every_n <n>\n"
            << "  --health_port <port>\n"
            << "  --record_path <file>\n"
            << "  --server_build_id <id>\n";
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

  mdd::InstrumentsConfig config_proto;
  std::string config_error;
  if (!mdd::common::LoadConfigFromJson(options.config_path, &config_proto, &config_error)) {
    std::cerr << "config error: " << config_error << "\n";
    return 1;
  }

  mdd::server::RuntimeConfig runtime_config = mdd::server::BuildRuntimeConfig(config_proto);

  mdd::server::ServiceOptions service_options;
  service_options.queue_limit_per_instrument = options.queue_limit_per_instrument;
  service_options.drop_every_n = options.drop_every_n;
  service_options.record_path = options.record_path;
  service_options.server_build_id = options.server_build_id;

  mdd::server::MarketDataServiceImpl service(runtime_config, options.seed, service_options);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(options.address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  if (!server) {
    std::cerr << "failed to start grpc server\n";
    return 1;
  }

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  HealthHttpServer health_server;
  if (options.health_port > 0) {
    std::string health_error;
    if (!health_server.Start(options.health_port, &health_error)) {
      std::cerr << "health endpoint error: " << health_error << "\n";
      return 1;
    }
  }

  mdd::common::Logger::Instance().Log(
      "server_start",
      {{"config_path", options.config_path},
       {"seed", mdd::common::ToString(options.seed)},
       {"address", options.address},
       {"health_port", mdd::common::ToString(static_cast<uint64_t>(options.health_port))},
       {"drop_every_n", mdd::common::ToString(static_cast<uint64_t>(options.drop_every_n))}});

  service.StartSimulation();

  while (!g_shutdown_requested.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  mdd::common::Logger::Instance().Log("server_shutdown_requested", {});
  service.StopSimulation();
  health_server.Stop();
  server->Shutdown();
  return 0;
}
