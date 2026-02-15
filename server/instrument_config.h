#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "mdd.pb.h"

namespace mdd::server {

struct InstrumentRuntimeConfig {
  std::string instrument_id;
  std::string symbol;
  uint32_t publish_depth = 10;
  int64_t tick_size = 1;
  int64_t base_price = 10000;
  uint32_t levels_per_side = 20;
  uint32_t updates_per_sec = 100;
  double volatility = 0.05;
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
