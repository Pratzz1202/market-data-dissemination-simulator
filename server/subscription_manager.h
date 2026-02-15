#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace mdd::server {

class SubscriptionManager {
 public:
  void AddClient(const std::string& client_id);
  std::vector<std::string> RemoveClient(const std::string& client_id);

  bool Subscribe(const std::string& client_id, const std::string& instrument_id,
                 uint32_t requested_depth, uint64_t subscription_id);
  bool Unsubscribe(const std::string& client_id, const std::string& instrument_id);

  bool IsSubscribed(const std::string& client_id, const std::string& instrument_id) const;

  std::vector<std::string> SubscribersFor(const std::string& instrument_id) const;
  std::vector<std::string> SubscriptionsForClient(const std::string& client_id) const;

  uint32_t RequestedDepth(const std::string& client_id, const std::string& instrument_id) const;
  uint64_t SubscriptionId(const std::string& client_id, const std::string& instrument_id) const;

  size_t ConnectedClients() const;

 private:
  struct Entry {
    uint32_t requested_depth = 0;
    uint64_t subscription_id = 0;
  };

  mutable std::mutex mu_;
  std::unordered_map<std::string, std::unordered_map<std::string, Entry>> client_subscriptions_;
  std::unordered_map<std::string, std::unordered_set<std::string>> instrument_subscribers_;
};

}  // namespace mdd::server
