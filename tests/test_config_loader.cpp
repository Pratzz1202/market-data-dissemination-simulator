#include <cstdio>
#include <filesystem>
#include <fstream>
#include <string>

#include "common/config_loader.h"
#include "common/logging.h"
#include "server/instrument_config.h"
#include "tests/test_util.h"

namespace {

mdd::InstrumentsConfig ValidConfig() {
  mdd::InstrumentsConfig config;
  auto* instrument = config.add_instruments();
  instrument->set_instrument_id("BTC-USD");
  instrument->set_publish_depth(10);
  instrument->set_tick_size(1);
  instrument->set_base_price(5000000);
  return config;
}

int TestValidation() {
  std::string error;

  mdd::InstrumentsConfig empty;
  CHECK(!mdd::common::ValidateConfig(empty, &error));

  CHECK(mdd::common::ValidateConfig(ValidConfig(), &error));

  auto no_depth = ValidConfig();
  no_depth.mutable_instruments(0)->set_publish_depth(0);
  CHECK(!mdd::common::ValidateConfig(no_depth, &error));

  auto no_tick = ValidConfig();
  no_tick.mutable_instruments(0)->set_tick_size(0);
  CHECK(!mdd::common::ValidateConfig(no_tick, &error));

  auto no_base = ValidConfig();
  no_base.mutable_instruments(0)->set_base_price(0);
  CHECK(!mdd::common::ValidateConfig(no_base, &error));

  auto negative_vol = ValidConfig();
  negative_vol.mutable_instruments(0)->set_volatility(-0.5);
  CHECK(!mdd::common::ValidateConfig(negative_vol, &error));

  auto duplicate = ValidConfig();
  *duplicate.add_instruments() = duplicate.instruments(0);
  CHECK(!mdd::common::ValidateConfig(duplicate, &error));
  CHECK(error.find("duplicate") != std::string::npos);

  return 0;
}

int TestRuntimeDefaults() {
  auto config = ValidConfig();
  config.set_default_updates_per_sec(0);  // falls back to 100

  const auto runtime = mdd::server::BuildRuntimeConfig(config);
  CHECK(runtime.default_updates_per_sec == 100);
  CHECK(runtime.instruments.size() == 1);

  const auto& instrument = runtime.instruments[0];
  CHECK(instrument.symbol == "BTC-USD");     // defaults to instrument_id
  CHECK(instrument.levels_per_side == 20);   // defaults to 2 * publish_depth
  CHECK(instrument.updates_per_sec == 100);  // defaults to resolved default rate
  CHECK(instrument.volatility == 0.05);      // defaults when unset
  return 0;
}

int TestJsonLoading() {
  const auto dir = std::filesystem::temp_directory_path();
  const auto good_path =
      dir / ("mdd_config_good_" + mdd::common::ToString(mdd::common::NowNs()) + ".json");
  const auto bad_path =
      dir / ("mdd_config_bad_" + mdd::common::ToString(mdd::common::NowNs()) + ".json");

  {
    std::ofstream out(good_path);
    out << R"({"instruments":[{"instrumentId":"BTC-USD","publishDepth":10,)"
        << R"("tickSize":1,"basePrice":5000000}]})";
  }
  {
    std::ofstream out(bad_path);
    out << "{ not json";
  }

  mdd::InstrumentsConfig config;
  std::string error;
  CHECK(mdd::common::LoadConfigFromJson(good_path.string(), &config, &error));
  CHECK(config.instruments_size() == 1);

  CHECK(!mdd::common::LoadConfigFromJson(bad_path.string(), &config, &error));
  CHECK(!error.empty());

  CHECK(!mdd::common::LoadConfigFromJson((dir / "mdd_missing.json").string(), &config, &error));

  std::filesystem::remove(good_path);
  std::filesystem::remove(bad_path);
  return 0;
}

}  // namespace

int main() {
  if (TestValidation() != 0) return 1;
  if (TestRuntimeDefaults() != 0) return 1;
  if (TestJsonLoading() != 0) return 1;

  std::cout << "test_config_loader passed\n";
  return 0;
}
