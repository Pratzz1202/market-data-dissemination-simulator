#include "client/apply_engine.h"

namespace mdd::client {

void ApplyEngine::SetSubscribed(const std::string& instrument_id, uint32_t depth) {
  std::lock_guard<std::mutex> lock(mu_);
  auto& state = states_[instrument_id];
  state.subscribed = true;
  state.depth = depth;
}

void ApplyEngine::OnUnsubscribed(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = states_.find(instrument_id);
  if (it == states_.end()) {
    return;
  }
  it->second.subscribed = false;
  it->second.has_seq = false;
  it->second.resync_inflight = false;
  it->second.last_seq = 0;
  it->second.book.Clear();
}

void ApplyEngine::OnSnapshot(const mdd::Snapshot& snapshot) {
  std::lock_guard<std::mutex> lock(mu_);
  auto& state = states_[snapshot.instrument_id()];
  state.subscribed = true;
  state.has_seq = true;
  state.resync_inflight = false;
  state.last_seq = snapshot.snapshot_seq();
  if (state.depth == 0) {
    state.depth = static_cast<uint32_t>(std::max(snapshot.bids_size(), snapshot.asks_size()));
  }
  state.book.ApplySnapshot(snapshot);
}

ApplyResult ApplyEngine::OnIncremental(const mdd::Incremental& incremental) {
  std::lock_guard<std::mutex> lock(mu_);
  ApplyResult result;

  auto it = states_.find(incremental.instrument_id());
  if (it == states_.end() || !it->second.subscribed) {
    result.decision = ApplyDecision::kIgnored;
    result.reason = "NOT_SUBSCRIBED";
    return result;
  }

  auto& state = it->second;

  if (!state.has_seq) {
    result.decision = ApplyDecision::kNeedResync;
    result.reason = "MISSING_SNAPSHOT";
    return result;
  }

  if (incremental.seq() <= state.last_seq) {
    result.decision = ApplyDecision::kNeedResync;
    result.reason = "OUT_OF_ORDER_OR_DUPLICATE";
    result.expected_prev = state.last_seq;
    result.got_prev = incremental.prev_seq();
    return result;
  }

  if (incremental.prev_seq() != state.last_seq) {
    result.decision = ApplyDecision::kNeedResync;
    result.reason = "GAP";
    result.expected_prev = state.last_seq;
    result.got_prev = incremental.prev_seq();
    return result;
  }

  std::string apply_error;
  if (!state.book.ApplyIncremental(incremental, &apply_error)) {
    result.decision = ApplyDecision::kNeedResync;
    result.reason = "APPLY_FAILED:" + apply_error;
    result.expected_prev = state.last_seq;
    result.got_prev = incremental.prev_seq();
    return result;
  }

  state.last_seq = incremental.seq();
  result.decision = ApplyDecision::kApplied;
  result.reason = "APPLIED";
  result.seq = incremental.seq();
  return result;
}

bool ApplyEngine::BeginResync(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = states_.find(instrument_id);
  if (it == states_.end()) {
    return false;
  }
  if (it->second.resync_inflight) {
    return false;
  }
  it->second.resync_inflight = true;
  return true;
}

void ApplyEngine::EndResync(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = states_.find(instrument_id);
  if (it == states_.end()) {
    return;
  }
  it->second.resync_inflight = false;
}

std::optional<InstrumentState> ApplyEngine::GetState(const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  const auto it = states_.find(instrument_id);
  if (it == states_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::vector<std::string> ApplyEngine::SubscribedInstruments() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::string> instruments;
  for (const auto& [instrument_id, state] : states_) {
    if (state.subscribed) {
      instruments.push_back(instrument_id);
    }
  }
  return instruments;
}

}  // namespace mdd::client
