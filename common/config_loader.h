#pragma once

#include <string>

#include "mdd.pb.h"

namespace mdd::common {

bool LoadConfigFromJson(const std::string& path, mdd::InstrumentsConfig* config,
                        std::string* error);

bool ValidateConfig(const mdd::InstrumentsConfig& config, std::string* error);

}  // namespace mdd::common
