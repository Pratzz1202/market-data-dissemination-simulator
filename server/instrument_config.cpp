#include "server/instrument_config.h"

namespace mdd::server {

RuntimeConfig BuildRuntimeConfig(const mdd::InstrumentsConfig& config) {
  RuntimeConfig runtime;
  runtime.default_updates_per_sec =
      config.default_updates_per_sec() == 0 ? 100 : config.default_updates_per_sec();
  runtime.reset_probability_ppm = config.reset_probability_ppm();
  runtime.allow_crossed_books = config.allow_crossed_books();

  runtime.instruments.reserve(config.instruments_size());
  for (const auto& instrument : config.instruments()) {
    InstrumentRuntimeConfig cfg;
    cfg.instrument_id = instrument.instrument_id();
    cfg.symbol = instrument.symbol().empty() ? instrument.instrument_id() : instrument.symbol();
    cfg.publish_depth = instrument.publish_depth();
    cfg.tick_size = instrument.tick_size();
    cfg.base_price = instrument.base_price();
    cfg.levels_per_side = instrument.levels_per_side() == 0 ? instrument.publish_depth() * 2
                                                            : instrument.levels_per_side();
    cfg.updates_per_sec = instrument.updates_per_sec() == 0 ? runtime.default_updates_per_sec
                                                            : instrument.updates_per_sec();
    cfg.volatility = instrument.volatility() == 0.0 ? 0.05 : instrument.volatility();
    runtime.instruments.push_back(cfg);
  }

  return runtime;
}

std::unordered_map<std::string, InstrumentRuntimeConfig> BuildInstrumentIndex(
    const RuntimeConfig& config) {
  std::unordered_map<std::string, InstrumentRuntimeConfig> index;
  index.reserve(config.instruments.size());
  for (const auto& instrument : config.instruments) {
    index[instrument.instrument_id] = instrument;
  }
  return index;
}

}  // namespace mdd::server
