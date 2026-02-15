#include "server/publisher.h"

#include <optional>
#include <utility>

#include "common/logging.h"
#include "common/metrics.h"

namespace mdd::server {

namespace {
constexpr const char* kControlChannel = "__control__";
}

ClientConnection::ClientConnection(const std::string& client_id,
                                   grpc::ServerReaderWriter<mdd::ServerMsg, mdd::ClientMsg>* stream,
                                   size_t per_instrument_queue_limit)
    : client_id_(client_id),
      stream_(stream),
      per_instrument_queue_limit_(per_instrument_queue_limit) {}

const std::string& ClientConnection::ClientId() const { return client_id_; }

bool ClientConnection::Enqueue(const std::string& instrument_id, const mdd::ServerMsg& msg,
                               bool is_incremental) {
  std::lock_guard<std::mutex> lock(mu_);
  if (closed_) {
    return false;
  }

  auto& queue = queues_[instrument_id];
  if (is_incremental && queue.size() >= per_instrument_queue_limit_) {
    return false;
  }

  if (!is_incremental && queue.size() >= per_instrument_queue_limit_) {
    queue.clear();
  }

  queue.push_back(msg);
  if (ready_set_.insert(instrument_id).second) {
    ready_.push_back(instrument_id);
  }
  common::GlobalMetrics().SetClientLag(client_id_, TotalPendingLocked());
  cv_.notify_one();
  return true;
}

bool ClientConnection::EnqueueReset(const std::string& instrument_id, const mdd::ServerMsg& msg) {
  std::lock_guard<std::mutex> lock(mu_);
  if (closed_) {
    return false;
  }
  auto& queue = queues_[instrument_id];
  queue.clear();
  queue.push_back(msg);
  if (ready_set_.insert(instrument_id).second) {
    ready_.push_back(instrument_id);
  }
  common::GlobalMetrics().SetClientLag(client_id_, TotalPendingLocked());
  cv_.notify_one();
  return true;
}

void ClientConnection::Close() {
  std::lock_guard<std::mutex> lock(mu_);
  closed_ = true;
  common::GlobalMetrics().SetClientLag(client_id_, 0);
  cv_.notify_all();
}

bool ClientConnection::IsClosed() const {
  std::lock_guard<std::mutex> lock(mu_);
  return closed_;
}

bool ClientConnection::IsDirty(const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  const auto it = dirty_.find(instrument_id);
  return it != dirty_.end() && it->second;
}

void ClientConnection::MarkDirty(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(mu_);
  dirty_[instrument_id] = true;
}

void ClientConnection::ClearDirty(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(mu_);
  dirty_[instrument_id] = false;
}

uint64_t ClientConnection::IncrementDropped(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(mu_);
  return ++dropped_counts_[instrument_id];
}

bool ClientConnection::PopNext(mdd::ServerMsg* msg) {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [&] { return closed_ || !ready_.empty(); });

  if (closed_ && ready_.empty()) {
    return false;
  }

  const std::string instrument_id = ready_.front();
  ready_.pop_front();
  ready_set_.erase(instrument_id);

  auto queue_it = queues_.find(instrument_id);
  if (queue_it == queues_.end() || queue_it->second.empty()) {
    return true;
  }

  *msg = queue_it->second.front();
  queue_it->second.pop_front();

  if (!queue_it->second.empty()) {
    ready_.push_back(instrument_id);
    ready_set_.insert(instrument_id);
  }

  common::GlobalMetrics().SetClientLag(client_id_, TotalPendingLocked());
  return true;
}

void ClientConnection::WriteLoop() {
  while (true) {
    mdd::ServerMsg msg;
    if (!PopNext(&msg)) {
      return;
    }

    if (!msg.has_server_hello() && !msg.has_snapshot() && !msg.has_incremental() &&
        !msg.has_unsubscribed() && !msg.has_error() && !msg.has_pong()) {
      continue;
    }

    if (!stream_->Write(msg)) {
      Close();
      return;
    }
  }
}

uint64_t ClientConnection::TotalPendingLocked() const {
  uint64_t total = 0;
  for (const auto& [_, queue] : queues_) {
    total += static_cast<uint64_t>(queue.size());
  }
  return total;
}

Publisher::Publisher(SubscriptionManager* subscriptions) : subscriptions_(subscriptions) {}

std::shared_ptr<ClientConnection> Publisher::RegisterClient(
    const std::string& client_id, grpc::ServerReaderWriter<mdd::ServerMsg, mdd::ClientMsg>* stream,
    size_t per_instrument_queue_limit) {
  auto connection =
      std::make_shared<ClientConnection>(client_id, stream, per_instrument_queue_limit);
  {
    std::lock_guard<std::mutex> lock(mu_);
    clients_[client_id] = connection;
  }
  common::GlobalMetrics().SetConnectedClients(subscriptions_->ConnectedClients());
  return connection;
}

void Publisher::UnregisterClient(const std::string& client_id) {
  std::lock_guard<std::mutex> lock(mu_);
  clients_.erase(client_id);
  common::GlobalMetrics().RemoveClientLag(client_id);
  common::GlobalMetrics().SetConnectedClients(subscriptions_->ConnectedClients());
}

void Publisher::SetSnapshotProvider(SnapshotProvider provider) {
  std::lock_guard<std::mutex> lock(mu_);
  snapshot_provider_ = std::move(provider);
}

std::shared_ptr<ClientConnection> Publisher::GetClient(const std::string& client_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  const auto it = clients_.find(client_id);
  if (it == clients_.end()) {
    return nullptr;
  }
  return it->second;
}

bool Publisher::SendControl(const std::string& client_id, const mdd::ServerMsg& msg) {
  const auto client = GetClient(client_id);
  if (client == nullptr) {
    return false;
  }
  return client->Enqueue(kControlChannel, msg, false);
}

bool Publisher::SendSnapshotToClient(const std::string& client_id, const mdd::Snapshot& snapshot) {
  const auto client = GetClient(client_id);
  if (client == nullptr) {
    return false;
  }

  mdd::ServerMsg msg;
  *msg.mutable_snapshot() = snapshot;
  const bool enqueued = client->EnqueueReset(snapshot.instrument_id(), msg);
  if (enqueued) {
    common::GlobalMetrics().IncrementSnapshots();
  }
  return enqueued;
}

bool Publisher::SendErrorToClient(const std::string& client_id, const std::string& instrument_id,
                                  const std::string& code, const std::string& message) {
  mdd::ServerMsg msg;
  auto* error = msg.mutable_error();
  error->set_instrument_id(instrument_id);
  error->set_code(code);
  error->set_message(message);
  return SendControl(client_id, msg);
}

void Publisher::PublishIncremental(const mdd::Incremental& incremental) {
  const auto subscribers = subscriptions_->SubscribersFor(incremental.instrument_id());
  std::vector<std::pair<std::string, std::shared_ptr<ClientConnection>>> targets;
  targets.reserve(subscribers.size());
  {
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& client_id : subscribers) {
      const auto it = clients_.find(client_id);
      if (it != clients_.end()) {
        targets.emplace_back(client_id, it->second);
      }
    }
  }

  SnapshotProvider provider;
  {
    std::lock_guard<std::mutex> lock(mu_);
    provider = snapshot_provider_;
  }
  std::optional<mdd::Snapshot> reset_snapshot_cache;

  for (const auto& [client_id, client] : targets) {
    if (client == nullptr || client->IsClosed()) {
      continue;
    }

    if (client->IsDirty(incremental.instrument_id())) {
      if (!reset_snapshot_cache.has_value() && provider) {
        reset_snapshot_cache = provider(incremental.instrument_id(), true, "BACKPRESSURE_RECOVERY");
      }
      if (reset_snapshot_cache.has_value()) {
        mdd::ServerMsg reset_msg;
        *reset_msg.mutable_snapshot() = *reset_snapshot_cache;
        if (client->EnqueueReset(incremental.instrument_id(), reset_msg)) {
          client->ClearDirty(incremental.instrument_id());
          common::GlobalMetrics().IncrementSnapshots();
        }
      }
      continue;
    }

    mdd::ServerMsg msg;
    *msg.mutable_incremental() = incremental;
    const bool enqueued = client->Enqueue(incremental.instrument_id(), msg, true);
    if (!enqueued) {
      client->MarkDirty(incremental.instrument_id());
      const uint64_t dropped = client->IncrementDropped(incremental.instrument_id());
      common::GlobalMetrics().IncrementBackpressureDrops();
      common::Logger::Instance().Log("backpressure_drop",
                                     {{"client_id", client_id},
                                      {"instrument_id", incremental.instrument_id()},
                                      {"dropped_count", common::ToString(dropped)}});
      continue;
    }
    common::GlobalMetrics().IncrementIncrementals();
  }
}

void Publisher::PublishSnapshotToSubscribers(const std::string& instrument_id,
                                             const mdd::Snapshot& snapshot) {
  const auto subscribers = subscriptions_->SubscribersFor(instrument_id);
  for (const auto& client_id : subscribers) {
    SendSnapshotToClient(client_id, snapshot);
  }
}

}  // namespace mdd::server
