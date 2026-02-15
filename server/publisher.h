#pragma once

#include <grpcpp/grpcpp.h>

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "mdd.grpc.pb.h"
#include "server/subscription_manager.h"

namespace mdd::server {

class ClientConnection {
 public:
  ClientConnection(const std::string& client_id,
                   grpc::ServerReaderWriter<mdd::ServerMsg, mdd::ClientMsg>* stream,
                   size_t per_instrument_queue_limit);

  const std::string& ClientId() const;

  bool Enqueue(const std::string& instrument_id, const mdd::ServerMsg& msg, bool is_incremental);
  bool EnqueueReset(const std::string& instrument_id, const mdd::ServerMsg& msg);
  void Close();
  bool IsClosed() const;

  bool IsDirty(const std::string& instrument_id) const;
  void MarkDirty(const std::string& instrument_id);
  void ClearDirty(const std::string& instrument_id);
  uint64_t IncrementDropped(const std::string& instrument_id);

  void WriteLoop();

 private:
  bool PopNext(mdd::ServerMsg* msg);
  uint64_t TotalPendingLocked() const;

  const std::string client_id_;
  grpc::ServerReaderWriter<mdd::ServerMsg, mdd::ClientMsg>* stream_;
  const size_t per_instrument_queue_limit_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  bool closed_ = false;

  std::unordered_map<std::string, std::deque<mdd::ServerMsg>> queues_;
  std::unordered_map<std::string, bool> dirty_;
  std::unordered_map<std::string, uint64_t> dropped_counts_;

  std::deque<std::string> ready_;
  std::unordered_set<std::string> ready_set_;
};

class Publisher {
 public:
  using SnapshotProvider =
      std::function<mdd::Snapshot(const std::string&, bool, const std::string&)>;

  explicit Publisher(SubscriptionManager* subscriptions);

  std::shared_ptr<ClientConnection> RegisterClient(
      const std::string& client_id,
      grpc::ServerReaderWriter<mdd::ServerMsg, mdd::ClientMsg>* stream,
      size_t per_instrument_queue_limit);

  void UnregisterClient(const std::string& client_id);

  void SetSnapshotProvider(SnapshotProvider provider);

  bool SendControl(const std::string& client_id, const mdd::ServerMsg& msg);
  bool SendSnapshotToClient(const std::string& client_id, const mdd::Snapshot& snapshot);
  bool SendErrorToClient(const std::string& client_id, const std::string& instrument_id,
                         const std::string& code, const std::string& message);

  void PublishIncremental(const mdd::Incremental& incremental);
  void PublishSnapshotToSubscribers(const std::string& instrument_id,
                                    const mdd::Snapshot& snapshot);

 private:
  std::shared_ptr<ClientConnection> GetClient(const std::string& client_id) const;

  SubscriptionManager* subscriptions_;
  mutable std::mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<ClientConnection>> clients_;
  SnapshotProvider snapshot_provider_;
};

}  // namespace mdd::server
