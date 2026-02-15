#include "server/simulator.h"

#include <algorithm>
#include <cmath>
#include <stdexcept>

#include "common/logging.h"

namespace mdd::server {

namespace {

template <typename Map>
int64_t PickPriceFromMap(const Map& levels, std::mt19937_64* rng) {
  if (levels.empty()) {
    return 0;
  }
  std::uniform_int_distribution<size_t> index_dist(0, levels.size() - 1);
  const auto pick = index_dist(*rng);
  size_t idx = 0;
  for (const auto& [price, _] : levels) {
    if (idx == pick) {
      return price;
    }
    ++idx;
  }
  return levels.begin()->first;
}

}  // namespace

Simulator::Simulator(RuntimeConfig config, uint64_t seed) : config_(std::move(config)) {
  std::lock_guard<std::mutex> lock(mu_);
  states_.reserve(config_.instruments.size());
  for (size_t i = 0; i < config_.instruments.size(); ++i) {
    const auto& cfg = config_.instruments[i];
    InstrumentState state;
    state.config = cfg;
    state.mid_price = cfg.base_price;
    state.rng.seed(seed + static_cast<uint64_t>(i * 9973 + 1));
    SeedInitialBook(&state);
    states_.emplace(cfg.instrument_id, std::move(state));
  }
}

bool Simulator::HasInstrument(const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  return states_.find(instrument_id) != states_.end();
}

std::vector<std::string> Simulator::InstrumentIds() const {
  std::lock_guard<std::mutex> lock(mu_);
  std::vector<std::string> ids;
  ids.reserve(states_.size());
  for (const auto& [instrument_id, _] : states_) {
    ids.push_back(instrument_id);
  }
  std::sort(ids.begin(), ids.end());
  return ids;
}

uint32_t Simulator::UpdatesPerSec(const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  const auto it = states_.find(instrument_id);
  if (it == states_.end()) {
    return config_.default_updates_per_sec;
  }
  return it->second.config.updates_per_sec;
}

uint64_t Simulator::CurrentSeq(const std::string& instrument_id) const {
  std::lock_guard<std::mutex> lock(mu_);
  const auto it = states_.find(instrument_id);
  if (it == states_.end()) {
    return 0;
  }
  return it->second.seq;
}

int64_t Simulator::ClampPositive(int64_t value, int64_t fallback) {
  if (value <= 0) {
    return fallback <= 0 ? 1 : fallback;
  }
  return value;
}

void Simulator::SeedInitialBook(InstrumentState* state) {
  const int64_t tick = state->config.tick_size;
  const uint32_t levels = std::max<uint32_t>(state->config.levels_per_side, 4);
  for (uint32_t i = 0; i < levels; ++i) {
    const int64_t bid_price =
        ClampPositive(state->config.base_price - static_cast<int64_t>((i + 1) * tick), tick);
    const int64_t ask_price = state->config.base_price + static_cast<int64_t>((i + 1) * tick);

    BookUpdate bid{Side::kBid, UpdateType::kUpsert, bid_price,
                   static_cast<int64_t>(500 + (levels - i) * 10)};
    BookUpdate ask{Side::kAsk, UpdateType::kUpsert, ask_price,
                   static_cast<int64_t>(500 + (levels - i) * 10)};
    state->book.Apply(bid);
    state->book.Apply(ask);
  }
}

BookUpdate Simulator::GenerateBookUpdate(InstrumentState* state) {
  std::uniform_int_distribution<int> side_dist(0, 1);
  std::uniform_int_distribution<int> action_dist(0, 99);
  std::uniform_int_distribution<int64_t> size_dist(1, 2500);
  std::uniform_int_distribution<int> drift_dist(-1, 1);

  const bool is_bid = side_dist(state->rng) == 0;
  const Side side = is_bid ? Side::kBid : Side::kAsk;

  // Small random walk around base with volatility-driven drift.
  const int64_t drift_tick = drift_dist(state->rng);
  if (std::abs(drift_tick) > 0) {
    const int64_t drift_scale =
        std::max<int64_t>(1, static_cast<int64_t>(state->config.volatility * 10));
    state->mid_price += drift_tick * state->config.tick_size * drift_scale;
    state->mid_price = ClampPositive(state->mid_price, state->config.tick_size * 100);
  }

  const bool can_remove = (side == Side::kBid && state->book.BidLevels() > 2) ||
                          (side == Side::kAsk && state->book.AskLevels() > 2);
  const bool do_remove = can_remove && action_dist(state->rng) < 20;

  if (do_remove) {
    const int64_t price = side == Side::kBid ? PickPriceFromMap(state->book.Bids(), &state->rng)
                                             : PickPriceFromMap(state->book.Asks(), &state->rng);
    return BookUpdate{side, UpdateType::kRemove, price, 0};
  }

  const uint32_t max_level = std::max<uint32_t>(state->config.levels_per_side, 4);
  std::uniform_int_distribution<uint32_t> level_dist(0, max_level - 1);
  const int64_t level_offset = static_cast<int64_t>(level_dist(state->rng));

  int64_t price = 0;
  if (side == Side::kBid) {
    const auto best_ask =
        state->book.BestAsk().value_or(state->mid_price + state->config.tick_size * 2);
    const int64_t max_bid = best_ask - state->config.tick_size;
    price = max_bid - level_offset * state->config.tick_size;
  } else {
    const auto best_bid =
        state->book.BestBid().value_or(state->mid_price - state->config.tick_size * 2);
    const int64_t min_ask = best_bid + state->config.tick_size;
    price = min_ask + level_offset * state->config.tick_size;
  }

  price = ClampPositive(price, state->config.tick_size);

  return BookUpdate{side, UpdateType::kUpsert, price, size_dist(state->rng)};
}

mdd::Incremental Simulator::GenerateIncremental(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = states_.find(instrument_id);
  if (it == states_.end()) {
    throw std::runtime_error("unknown instrument: " + instrument_id);
  }

  InstrumentState& state = it->second;
  BookUpdate update = GenerateBookUpdate(&state);

  std::string error;
  if (!state.book.Apply(update, &error)) {
    throw std::runtime_error("failed to apply simulated update: " + error);
  }

  if (!config_.allow_crossed_books && state.book.IsCrossed()) {
    // Roll back the aggressive update and place a safe top-of-book level.
    state.book.Apply(BookUpdate{update.side, UpdateType::kRemove, update.price, 0});
    if (update.side == Side::kBid) {
      const auto best_ask =
          state.book.BestAsk().value_or(state.mid_price + state.config.tick_size * 2);
      const int64_t safe_bid = best_ask - state.config.tick_size;
      state.book.Apply(BookUpdate{Side::kBid, UpdateType::kUpsert,
                                  ClampPositive(safe_bid, state.config.tick_size),
                                  std::max<int64_t>(1, update.size)});
      update.price = ClampPositive(safe_bid, state.config.tick_size);
    } else {
      const auto best_bid =
          state.book.BestBid().value_or(state.mid_price - state.config.tick_size * 2);
      const int64_t safe_ask = best_bid + state.config.tick_size;
      state.book.Apply(BookUpdate{Side::kAsk, UpdateType::kUpsert, ClampPositive(safe_ask, 1),
                                  std::max<int64_t>(1, update.size)});
      update.price = ClampPositive(safe_ask, 1);
    }
  }

  const uint64_t prev_seq = state.seq;
  state.seq++;
  state.ticks++;

  mdd::Incremental incremental;
  incremental.set_instrument_id(instrument_id);
  incremental.set_prev_seq(prev_seq);
  incremental.set_seq(state.seq);
  incremental.set_server_timestamp_ns(common::NowNs());
  *incremental.add_updates() = ToProto(update);
  return incremental;
}

mdd::Snapshot Simulator::BuildSnapshot(const std::string& instrument_id, uint32_t depth_override,
                                       bool is_reset, const std::string& reason) const {
  std::lock_guard<std::mutex> lock(mu_);
  const auto it = states_.find(instrument_id);
  if (it == states_.end()) {
    throw std::runtime_error("unknown instrument: " + instrument_id);
  }

  const InstrumentState& state = it->second;
  const uint32_t depth = depth_override == 0 ? state.config.publish_depth : depth_override;

  mdd::Snapshot snapshot;
  snapshot.set_instrument_id(instrument_id);
  snapshot.set_snapshot_seq(state.seq);
  snapshot.set_is_reset(is_reset);
  snapshot.set_server_timestamp_ns(common::NowNs());
  snapshot.set_reason(reason);

  for (const auto& bid : state.book.TopBids(depth)) {
    auto* level = snapshot.add_bids();
    level->set_price(bid.price);
    level->set_size(bid.size);
  }
  for (const auto& ask : state.book.TopAsks(depth)) {
    auto* level = snapshot.add_asks();
    level->set_price(ask.price);
    level->set_size(ask.size);
  }

  return snapshot;
}

bool Simulator::ShouldEmitReset(const std::string& instrument_id) {
  std::lock_guard<std::mutex> lock(mu_);
  const auto it = states_.find(instrument_id);
  if (it == states_.end()) {
    return false;
  }
  if (config_.reset_probability_ppm == 0) {
    return false;
  }

  std::uniform_int_distribution<uint32_t> dist(0, 999999);
  return dist(it->second.rng) < config_.reset_probability_ppm;
}

}  // namespace mdd::server
