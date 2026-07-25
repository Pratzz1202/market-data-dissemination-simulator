#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "mdd.pb.h"

namespace mdd::server {

// Populated by BuildRuntimeConfig, which resolves every field from the proto
// config (member initializers here would be dead code masquerading as
// defaults). publish_depth, tick_size, and base_price are required by
// ValidateConfig; the rest fall back inside BuildRuntimeConfig.
struct InstrumentRuntimeConfig {
  std::string instrument_id;
  std::string symbol;
  uint32_t publish_depth = 0;
  int64_t tick_size = 0;
  int64_t base_price = 0;
  uint32_t levels_per_side = 0;
  uint32_t updates_per_sec = 0;
  double volatility = 0.0;
};

struct RuntimeConfig {
  std::vector<InstrumentRuntimeConfig> instruments;
  uint32_t default_updates_per_sec = 100;
  uint32_t reset_probability_ppm = 100;
  bool allow_crossed_books = false;
};

RuntimeConfig BuildRuntimeConfig(const mdd::InstrumentsConfig& config);

std::unordered_map<std::string, InstrumentRuntimeConfig> BuildInstrumentIndex(
    const RuntimeConfig& config);

}  // namespace mdd::server
