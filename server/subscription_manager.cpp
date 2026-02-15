#include "server/subscription_manager.h"

namespace mdd::server {

void SubscriptionManager::AddClient(const std::string& client_id) {
  std::lock_guard<std::mutex> lock(mu_);
  client_subscriptions_.try_emplace(client_id);
}

std::vector<std::string> SubscriptionManager::RemoveClient(const std::string& client_id) {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::string> removed;

  const auto client_it = client_subscriptions_.find(client_id);
  if (client_it == client_subscriptions_.end()) {
    return removed;
  }

  removed.reserve(client_it->second.size());
  for (const auto& [instrument_id, _] : client_it->second) {
    removed.push_back(instrument_id);
    auto subscribers_it = instrument_subscribers_.find(instrument_id);
    if (subscribers_it != instrument_subscribers_.end()) {
      subscribers_it->second.erase(client_id);
      if (subscribers_it->second.empty()) {
        instrument_subscribers_.erase(subscribers_it);
      }
    }
  }

  client_subscriptions_.erase(client_it);
  return removed;
}

bool SubscriptionManager::Subscribe(const std::string& client_id, const std::string& instrument_id,
                                    uint32_t requested_depth, uint64_t subscription_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto client_it = client_subscriptions_.find(client_id);
  if (client_it == client_subscriptions_.end()) {
    return false;
  }

  auto [_, inserted] =
      client_it->second.emplace(instrument_id, Entry{requested_depth, subscription_id});
  if (!inserted) {
    return false;
  }

  instrument_subscribers_[instrument_id].insert(client_id);
  return true;
}

bool SubscriptionManager::Unsubscribe(const std::string& client_id,
                                      const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto client_it = client_subscriptions_.find(client_id);
  if (client_it == client_subscriptions_.end()) {
    return false;
  }

  const auto erased = client_it->second.erase(instrument_id);
  if (erased == 0) {
    return false;
  }

  auto subscribers_it = instrument_subscribers_.find(instrument_id);
  if (subscribers_it != instrument_subscribers_.end()) {
    subscribers_it->second.erase(client_id);
    if (subscribers_it->second.empty()) {
      instrument_subscribers_.erase(subscribers_it);
    }
  }

  return true;
}

bool SubscriptionManager::IsSubscribed(const std::string& client_id,
                                       const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  const auto client_it = client_subscriptions_.find(client_id);
  if (client_it == client_subscriptions_.end()) {
    return false;
  }
  return client_it->second.find(instrument_id) != client_it->second.end();
}

std::vector<std::string> SubscriptionManager::SubscribersFor(
    const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::string> subscribers;
  const auto it = instrument_subscribers_.find(instrument_id);
  if (it == instrument_subscribers_.end()) {
    return subscribers;
  }
  subscribers.reserve(it->second.size());
  for (const auto& client_id : it->second) {
    subscribers.push_back(client_id);
  }
  return subscribers;
}

std::vector<std::string> SubscriptionManager::SubscriptionsForClient(
    const std::string& client_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::string> instruments;
  const auto it = client_subscriptions_.find(client_id);
  if (it == client_subscriptions_.end()) {
    return instruments;
  }
  instruments.reserve(it->second.size());
  for (const auto& [instrument_id, _] : it->second) {
    instruments.push_back(instrument_id);
  }
  return instruments;
}

uint32_t SubscriptionManager::RequestedDepth(const std::string& client_id,
                                             const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  const auto client_it = client_subscriptions_.find(client_id);
  if (client_it == client_subscriptions_.end()) {
    return 0;
  }
  const auto instrument_it = client_it->second.find(instrument_id);
  if (instrument_it == client_it->second.end()) {
    return 0;
  }
  return instrument_it->second.requested_depth;
}

uint64_t SubscriptionManager::SubscriptionId(const std::string& client_id,
                                             const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  const auto client_it = client_subscriptions_.find(client_id);
  if (client_it == client_subscriptions_.end()) {
    return 0;
  }
  const auto instrument_it = client_it->second.find(instrument_id);
  if (instrument_it == client_it->second.end()) {
    return 0;
  }
  return instrument_it->second.subscription_id;
}

size_t SubscriptionManager::ConnectedClients() const {
  std::lock_guard<std::mutex> lock(mu_);
  return client_subscriptions_.size();
}

}  // namespace mdd::server
