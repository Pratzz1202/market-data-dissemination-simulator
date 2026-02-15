#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "client/order_book_view.h"
#include "mdd.pb.h"

namespace mdd::client {

enum class ApplyDecision { kApplied, kNeedResync, kIgnored };

struct ApplyResult {
  ApplyDecision decision = ApplyDecision::kIgnored;
  std::string reason;
  uint64_t expected_prev = 0;
  uint64_t got_prev = 0;
  uint64_t seq = 0;
};

struct InstrumentState {
  bool subscribed = false;
  bool has_seq = false;
  bool resync_inflight = false;
  uint64_t last_seq = 0;
  uint32_t depth = 0;
  OrderBookView book;
};

class ApplyEngine {
 public:
  void SetSubscribed(const std::string& instrument_id, uint32_t depth);
  void OnUnsubscribed(const std::string& instrument_id);

  void OnSnapshot(const mdd::Snapshot& snapshot);
  ApplyResult OnIncremental(const mdd::Incremental& incremental);

  bool BeginResync(const std::string& instrument_id);
  void EndResync(const std::string& instrument_id);

  std::optional<InstrumentState> GetState(const std::string& instrument_id) const;
  std::vector<std::string> SubscribedInstruments() const;

 private:
  mutable std::mutex mu_;
  std::unordered_map<std::string, InstrumentState> states_;
};

}  // namespace mdd::client
