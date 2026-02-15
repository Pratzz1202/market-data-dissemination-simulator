#include <iostream>

#include "client/apply_engine.h"

namespace {

#define CHECK(expr)                                                        \
  do {                                                                     \
    if (!(expr)) {                                                         \
      std::cerr << "CHECK failed at line " << __LINE__ << ": " #expr "\n"; \
      return 1;                                                            \
    }                                                                      \
  } while (false)

mdd::Snapshot BuildSnapshot(const std::string& instrument_id, uint64_t seq) {
  mdd::Snapshot snapshot;
  snapshot.set_instrument_id(instrument_id);
  snapshot.set_snapshot_seq(seq);

  auto* bid = snapshot.add_bids();
  bid->set_price(10000);
  bid->set_size(10);

  auto* ask = snapshot.add_asks();
  ask->set_price(10001);
  ask->set_size(12);
  return snapshot;
}

mdd::Incremental BuildIncremental(const std::string& instrument_id, uint64_t prev, uint64_t seq) {
  mdd::Incremental inc;
  inc.set_instrument_id(instrument_id);
  inc.set_prev_seq(prev);
  inc.set_seq(seq);

  auto* update = inc.add_updates();
  update->set_side(mdd::SIDE_BID);
  update->set_op(mdd::DELTA_OP_UPSERT);
  update->set_price(9999);
  update->set_size(15);
  return inc;
}

int TestHappyPath() {
  mdd::client::ApplyEngine engine;
  engine.SetSubscribed("BTC-USD", 10);

  engine.OnSnapshot(BuildSnapshot("BTC-USD", 100));

  auto result = engine.OnIncremental(BuildIncremental("BTC-USD", 100, 101));
  CHECK(result.decision == mdd::client::ApplyDecision::kApplied);

  const auto state = engine.GetState("BTC-USD");
  CHECK(state.has_value());
  CHECK(state->last_seq == 101);
  CHECK(state->has_seq);
  return 0;
}

int TestGapDetection() {
  mdd::client::ApplyEngine engine;
  engine.SetSubscribed("BTC-USD", 10);
  engine.OnSnapshot(BuildSnapshot("BTC-USD", 200));

  auto result = engine.OnIncremental(BuildIncremental("BTC-USD", 198, 201));
  CHECK(result.decision == mdd::client::ApplyDecision::kNeedResync);
  CHECK(result.reason == "GAP");
  CHECK(result.expected_prev == 200);
  CHECK(result.got_prev == 198);
  return 0;
}

int TestDuplicateDetection() {
  mdd::client::ApplyEngine engine;
  engine.SetSubscribed("ETH-USD", 10);
  engine.OnSnapshot(BuildSnapshot("ETH-USD", 10));

  auto result = engine.OnIncremental(BuildIncremental("ETH-USD", 10, 10));
  CHECK(result.decision == mdd::client::ApplyDecision::kNeedResync);
  CHECK(result.reason == "OUT_OF_ORDER_OR_DUPLICATE");
  return 0;
}

}  // namespace

int main() {
  if (TestHappyPath() != 0) return 1;
  if (TestGapDetection() != 0) return 1;
  if (TestDuplicateDetection() != 0) return 1;

  std::cout << "test_client_apply passed\n";
  return 0;
}
