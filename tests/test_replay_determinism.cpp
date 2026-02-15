#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "common/recording.h"
#include "server/order_book.h"
#include "server/simulator.h"

namespace {

#define CHECK(expr)                                                        \
  do {                                                                     \
    if (!(expr)) {                                                         \
      std::cerr << "CHECK failed at line " << __LINE__ << ": " #expr "\n"; \
      return 1;                                                            \
    }                                                                      \
  } while (false)

mdd::server::RuntimeConfig BuildRuntimeConfig() {
  mdd::server::RuntimeConfig config;
  config.default_updates_per_sec = 1000;
  config.reset_probability_ppm = 0;
  config.allow_crossed_books = false;

  mdd::server::InstrumentRuntimeConfig btc;
  btc.instrument_id = "BTC-USD";
  btc.symbol = "BTCUSD";
  btc.publish_depth = 10;
  btc.tick_size = 1;
  btc.base_price = 5000000;
  btc.levels_per_side = 40;
  btc.updates_per_sec = 1000;
  btc.volatility = 0.08;
  config.instruments.push_back(btc);

  mdd::server::InstrumentRuntimeConfig eth;
  eth.instrument_id = "ETH-USD";
  eth.symbol = "ETHUSD";
  eth.publish_depth = 10;
  eth.tick_size = 1;
  eth.base_price = 300000;
  eth.levels_per_side = 40;
  eth.updates_per_sec = 1000;
  eth.volatility = 0.06;
  config.instruments.push_back(eth);

  return config;
}

bool SameDelta(const mdd::LevelDelta& a, const mdd::LevelDelta& b) {
  return a.side() == b.side() && a.op() == b.op() && a.price() == b.price() && a.size() == b.size();
}

bool SameIncrementalIgnoringTimestamp(const mdd::Incremental& a, const mdd::Incremental& b) {
  if (a.instrument_id() != b.instrument_id() || a.seq() != b.seq() ||
      a.prev_seq() != b.prev_seq() || a.updates_size() != b.updates_size()) {
    return false;
  }
  for (int i = 0; i < a.updates_size(); ++i) {
    if (!SameDelta(a.updates(i), b.updates(i))) {
      return false;
    }
  }
  return true;
}

bool SamePriceLevel(const mdd::PriceLevel& a, const mdd::PriceLevel& b) {
  return a.price() == b.price() && a.size() == b.size();
}

bool SameSnapshotBookIgnoringMeta(const mdd::Snapshot& a, const mdd::Snapshot& b) {
  if (a.instrument_id() != b.instrument_id() || a.snapshot_seq() != b.snapshot_seq() ||
      a.bids_size() != b.bids_size() || a.asks_size() != b.asks_size()) {
    return false;
  }
  for (int i = 0; i < a.bids_size(); ++i) {
    if (!SamePriceLevel(a.bids(i), b.bids(i))) {
      return false;
    }
  }
  for (int i = 0; i < a.asks_size(); ++i) {
    if (!SamePriceLevel(a.asks(i), b.asks(i))) {
      return false;
    }
  }
  return true;
}

int TestDeterministicSequenceWithSeed() {
  const auto config = BuildRuntimeConfig();
  mdd::server::Simulator sim_a(config, 777);
  mdd::server::Simulator sim_b(config, 777);

  const std::vector<std::string> instruments = {"BTC-USD", "ETH-USD"};
  for (int i = 0; i < 300; ++i) {
    for (const auto& instrument : instruments) {
      const auto inc_a = sim_a.GenerateIncremental(instrument);
      const auto inc_b = sim_b.GenerateIncremental(instrument);
      CHECK(SameIncrementalIgnoringTimestamp(inc_a, inc_b));
    }
  }

  for (const auto& instrument : instruments) {
    const auto snap_a = sim_a.BuildSnapshot(instrument, 10, false, "CHECK");
    const auto snap_b = sim_b.BuildSnapshot(instrument, 10, false, "CHECK");
    CHECK(SameSnapshotBookIgnoringMeta(snap_a, snap_b));
  }

  return 0;
}

int TestRecordReplayFinalStateConsistency() {
  const auto config = BuildRuntimeConfig();
  mdd::server::Simulator simulator(config, 2024);

  const std::string record_path =
      "/tmp/mdd_replay_determinism_" + mdd::common::ToString(mdd::common::NowNs()) + ".record";
  mdd::common::Recorder recorder;
  std::string open_error;
  CHECK(recorder.Open(record_path, &open_error));

  for (int i = 0; i < 400; ++i) {
    auto inc = simulator.GenerateIncremental("BTC-USD");
    CHECK(recorder.RecordIncremental(inc));
  }

  const auto expected_snapshot = simulator.BuildSnapshot("BTC-USD", 10, false, "EXPECTED_FINAL");
  CHECK(recorder.RecordSnapshot(expected_snapshot));
  recorder.Close();

  mdd::common::RecordReader reader;
  CHECK(reader.Open(record_path, &open_error));

  mdd::server::OrderBook replay_book;
  bool has_seq = false;
  uint64_t last_seq = 0;

  while (true) {
    const auto maybe_event = reader.Next();
    if (!maybe_event.has_value()) {
      break;
    }

    const auto& event = maybe_event.value();
    if (event.has_incremental()) {
      const auto& incremental = event.incremental();
      CHECK(!has_seq || incremental.prev_seq() == last_seq);

      std::vector<mdd::server::BookUpdate> updates;
      updates.reserve(incremental.updates_size());
      for (const auto& delta : incremental.updates()) {
        updates.push_back(mdd::server::FromProto(delta));
      }

      std::string apply_error;
      CHECK(replay_book.ApplyBatch(updates, &apply_error));

      has_seq = true;
      last_seq = incremental.seq();
    } else if (event.has_snapshot()) {
      const auto& snapshot = event.snapshot();
      std::vector<mdd::server::Level> bids;
      bids.reserve(snapshot.bids_size());
      for (const auto& level : snapshot.bids()) {
        bids.push_back(mdd::server::Level{level.price(), level.size()});
      }
      std::vector<mdd::server::Level> asks;
      asks.reserve(snapshot.asks_size());
      for (const auto& level : snapshot.asks()) {
        asks.push_back(mdd::server::Level{level.price(), level.size()});
      }
      replay_book.Replace(bids, asks);
      has_seq = true;
      last_seq = snapshot.snapshot_seq();
    }
  }

  CHECK(has_seq);
  CHECK(last_seq == expected_snapshot.snapshot_seq());

  const auto replay_bids = replay_book.TopBids(10);
  const auto replay_asks = replay_book.TopAsks(10);
  CHECK(static_cast<int>(replay_bids.size()) == expected_snapshot.bids_size());
  CHECK(static_cast<int>(replay_asks.size()) == expected_snapshot.asks_size());
  for (int i = 0; i < expected_snapshot.bids_size(); ++i) {
    CHECK(replay_bids[static_cast<size_t>(i)].price == expected_snapshot.bids(i).price());
    CHECK(replay_bids[static_cast<size_t>(i)].size == expected_snapshot.bids(i).size());
  }
  for (int i = 0; i < expected_snapshot.asks_size(); ++i) {
    CHECK(replay_asks[static_cast<size_t>(i)].price == expected_snapshot.asks(i).price());
    CHECK(replay_asks[static_cast<size_t>(i)].size == expected_snapshot.asks(i).size());
  }

  std::remove(record_path.c_str());
  return 0;
}

}  // namespace

int main() {
  if (TestDeterministicSequenceWithSeed() != 0) return 1;
  if (TestRecordReplayFinalStateConsistency() != 0) return 1;

  std::cout << "test_replay_determinism passed\n";
  return 0;
}
