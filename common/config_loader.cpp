#include "common/config_loader.h"

#include <fstream>
#include <sstream>

#include "google/protobuf/util/json_util.h"

namespace mdd::common {

bool ValidateConfig(const mdd::InstrumentsConfig& config, std::string* error) {
  if (config.instruments_size() == 0) {
    if (error != nullptr) {
      *error = "config must contain at least one instrument";
    }
    return false;
  }

  for (const auto& instrument : config.instruments()) {
    if (instrument.instrument_id().empty()) {
      if (error != nullptr) {
        *error = "instrument_id cannot be empty";
      }
      return false;
    }
    if (instrument.publish_depth() == 0) {
      if (error != nullptr) {
        *error = "publish_depth must be > 0 for " + instrument.instrument_id();
      }
      return false;
    }
    if (instrument.tick_size() <= 0) {
      if (error != nullptr) {
        *error = "tick_size must be > 0 for " + instrument.instrument_id();
      }
      return false;
    }
    if (instrument.base_price() <= 0) {
      if (error != nullptr) {
        *error = "base_price must be > 0 for " + instrument.instrument_id();
      }
      return false;
    }
  }

  return true;
}

bool LoadConfigFromJson(const std::string& path, mdd::InstrumentsConfig* config,
                        std::string* error) {
  if (config == nullptr) {
    if (error != nullptr) {
      *error = "config pointer is null";
    }
    return false;
  }

  std::ifstream input(path);
  if (!input.is_open()) {
    if (error != nullptr) {
      *error = "failed to open config file: " + path;
    }
    return false;
  }

  std::ostringstream buffer;
  buffer << input.rdbuf();

  google::protobuf::util::JsonParseOptions options;
  options.ignore_unknown_fields = true;
  const auto status = google::protobuf::util::JsonStringToMessage(buffer.str(), config, options);
  if (!status.ok()) {
    if (error != nullptr) {
      *error = "json parse failure: " + std::string(status.message());
    }
    return false;
  }

  return ValidateConfig(*config, error);
}

}  // namespace mdd::common
