#pragma once

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "client/apply_engine.h"
#include "mdd.grpc.pb.h"

namespace mdd::client {

struct ClientSessionOptions {
  std::string target = "localhost:50051";
  bool auto_reconnect = true;
  uint32_t reconnect_delay_ms = 1000;
  uint32_t schema_version = 1;
  std::string client_build_id = "mdd_client_1.0";
  std::shared_ptr<grpc::Channel> channel_override;
  uint32_t incremental_processing_delay_ms = 0;
  bool verbose = true;
};

struct ClientSessionCallbacks {
  std::function<void(uint64_t)> on_incremental_latency_ns;
  std::function<void(uint64_t)> on_snapshot_latency_ns;
  std::function<void(uint64_t)> on_latency_ns;  // compatibility alias for incremental latency
  std::function<void()> on_incremental;
  std::function<void(const mdd::Incremental&)> on_incremental_msg;
  std::function<void(const mdd::Snapshot&)> on_snapshot;
  std::function<void(const std::string&)> on_resync_requested;
  std::function<void(const std::string&)> on_error;
  std::function<void()> on_connected;
  std::function<void()> on_disconnected;
};

class ClientSession {
 public:
  explicit ClientSession(ClientSessionOptions options);
  ~ClientSession();

  void Start();
  void Stop();

  void Subscribe(const std::string& instrument_id, uint32_t depth, uint64_t subscription_id = 0);
  void Unsubscribe(const std::string& instrument_id);
  void Ping();

  void SetCallbacks(ClientSessionCallbacks callbacks);

  std::optional<InstrumentState> GetInstrumentState(const std::string& instrument_id) const;

 private:
  struct DesiredSubscription {
    uint32_t depth = 0;
    uint64_t subscription_id = 0;
  };

  void Run();
  void Enqueue(const mdd::ClientMsg& msg);
  bool WriteInitialHandshake(grpc::ClientReaderWriter<mdd::ClientMsg, mdd::ServerMsg>* stream);
  bool WriteCurrentSubscriptions(grpc::ClientReaderWriter<mdd::ClientMsg, mdd::ServerMsg>* stream);

  void WriterLoop(grpc::ClientReaderWriter<mdd::ClientMsg, mdd::ServerMsg>* stream,
                  std::atomic<bool>* stream_active);
  void HandleServerMessage(const mdd::ServerMsg& msg);

  void RequestResync(const std::string& instrument_id, const std::string& reason,
                     uint64_t last_seq_seen);

  ClientSessionOptions options_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<mdd::MarketDataService::Stub> stub_;

  mutable std::mutex callbacks_mu_;
  ClientSessionCallbacks callbacks_;

  std::atomic<bool> stop_{false};
  std::atomic<bool> started_{false};
  std::atomic<bool> stream_connected_{false};
  std::thread run_thread_;

  mutable std::mutex queue_mu_;
  std::condition_variable queue_cv_;
  std::deque<mdd::ClientMsg> outbound_queue_;

  mutable std::mutex stream_ctx_mu_;
  grpc::ClientContext* active_context_ = nullptr;

  mutable std::mutex desired_mu_;
  std::unordered_map<std::string, DesiredSubscription> desired_subscriptions_;
  uint64_t subscription_counter_ = 1;

  ApplyEngine apply_engine_;
};

}  // namespace mdd::client
