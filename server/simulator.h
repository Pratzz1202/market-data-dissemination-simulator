#pragma once

#include <cstdint>
#include <mutex>
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

  // depth_override == 0 publishes the full book. Protocol snapshots must
  // cover every level the incremental stream can touch: a truncated snapshot
  // plus full-book incrementals silently desyncs clients the moment a remove
  // uncovers a level the snapshot never carried (sequence numbers stay valid,
  // so no resync ever fires). Non-zero depth is for display/tooling only.
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
    // Instruments are independent; a single simulator-wide mutex would
    // serialize every instrument thread through one hot lock.
    mutable std::mutex mu;
  };

  static int64_t ClampPositive(int64_t value, int64_t fallback);
  static BookUpdate GenerateBookUpdate(InstrumentState* state);
  static void SeedInitialBook(InstrumentState* state);

  const InstrumentState* FindState(const std::string& instrument_id) const;
  InstrumentState* FindState(const std::string& instrument_id);

  RuntimeConfig config_;
  // Keys and entries are immutable after construction; only the per-entry
  // state behind each InstrumentState::mu mutates.
  std::unordered_map<std::string, InstrumentState> states_;
};

}  // namespace mdd::server
