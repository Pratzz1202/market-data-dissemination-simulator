#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "mdd.pb.h"
#include "server/instrument_config.h"
#include "server/order_book.h"

namespace mdd::server {

class Simulator {
 public:
  Simulator(RuntimeConfig config, uint64_t seed);

  bool HasInstrument(const std::string& instrument_id) const;
  std::vector<std::string> InstrumentIds() const;

  uint32_t UpdatesPerSec(const std::string& instrument_id) const;
  uint64_t CurrentSeq(const std::string& instrument_id) const;

  mdd::Incremental GenerateIncremental(const std::string& instrument_id);
  mdd::Snapshot BuildSnapshot(const std::string& instrument_id, uint32_t depth_override = 0,
                              bool is_reset = false, const std::string& reason = "") const;

  bool ShouldEmitReset(const std::string& instrument_id);

 private:
  struct InstrumentState {
    InstrumentRuntimeConfig config;
    OrderBook book;
    uint64_t seq = 0;
    uint64_t ticks = 0;
    int64_t mid_price = 0;
    mutable std::mt19937_64 rng;
  };

  static int64_t ClampPositive(int64_t value, int64_t fallback);
  BookUpdate GenerateBookUpdate(InstrumentState* state);
  void SeedInitialBook(InstrumentState* state);

  RuntimeConfig config_;
  mutable std::mutex mu_;
  std::unordered_map<std::string, InstrumentState> states_;
};

}  // namespace mdd::server
