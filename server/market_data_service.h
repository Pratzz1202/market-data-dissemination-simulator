#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/recording.h"
#include "mdd.grpc.pb.h"
#include "server/instrument_config.h"
#include "server/publisher.h"
#include "server/simulator.h"
#include "server/subscription_manager.h"

namespace mdd::server {

struct ServiceOptions {
  std::string server_build_id = "mdd_server_1.0";
  size_t queue_limit_per_instrument = 256;
  uint32_t drop_every_n = 0;
  std::string record_path;
};

class MarketDataServiceImpl final : public mdd::MarketDataService::Service {
 public:
  MarketDataServiceImpl(RuntimeConfig runtime_config, uint64_t seed, ServiceOptions options);
  ~MarketDataServiceImpl() override;

  void StartSimulation();
  void StopSimulation();

  grpc::Status Stream(grpc::ServerContext* context,
                      grpc::ServerReaderWriter<mdd::ServerMsg, mdd::ClientMsg>* stream) override;

 private:
  void RunInstrumentLoop(const std::string& instrument_id);
  void HandleSubscribe(const std::string& client_id, const mdd::Subscribe& subscribe);
  void HandleUnsubscribe(const std::string& client_id, const mdd::Unsubscribe& unsubscribe);
  void HandleResync(const std::string& client_id, const mdd::ResyncRequest& resync_request);
  void HandlePing(const std::string& client_id, const mdd::Ping& ping);

  RuntimeConfig runtime_config_;
  ServiceOptions options_;

  Simulator simulator_;
  SubscriptionManager subscriptions_;
  Publisher publisher_;
  common::Recorder recorder_;

  std::atomic<bool> running_{false};
  std::vector<std::thread> simulator_threads_;
  std::atomic<uint64_t> client_counter_{0};
};

}  // namespace mdd::server
