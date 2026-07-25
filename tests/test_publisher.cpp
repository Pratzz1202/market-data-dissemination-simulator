#include <memory>
#include <string>

#include "server/publisher.h"
#include "server/subscription_manager.h"
#include "tests/test_util.h"

namespace {

// ClientConnection never dereferences its stream pointer outside WriteLoop,
// so queue behavior is unit-testable with a null stream.
constexpr size_t kQueueLimit = 4;

mdd::ServerMsg MakeIncrementalMsg(const std::string& instrument_id, uint64_t seq) {
  mdd::ServerMsg msg;
  auto* incremental = msg.mutable_incremental();
  incremental->set_instrument_id(instrument_id);
  incremental->set_seq(seq);
  incremental->set_prev_seq(seq - 1);
  return msg;
}

mdd::Incremental MakeIncremental(const std::string& instrument_id, uint64_t seq) {
  mdd::Incremental incremental;
  incremental.set_instrument_id(instrument_id);
  incremental.set_seq(seq);
  incremental.set_prev_seq(seq - 1);
  auto* update = incremental.add_updates();
  update->set_side(mdd::SIDE_BID);
  update->set_op(mdd::DELTA_OP_UPSERT);
  update->set_price(100);
  update->set_size(1);
  return incremental;
}

int TestIncrementalQueueBound() {
  mdd::server::ClientConnection connection("c1", nullptr, kQueueLimit);

  for (uint64_t seq = 1; seq <= kQueueLimit; ++seq) {
    CHECK(connection.Enqueue("X", MakeIncrementalMsg("X", seq), true));
  }
  CHECK(!connection.Enqueue("X", MakeIncrementalMsg("X", kQueueLimit + 1), true));
  CHECK(connection.TotalPending() == kQueueLimit);
  return 0;
}

int TestControlBypassesBound() {
  mdd::server::ClientConnection connection("c1", nullptr, kQueueLimit);

  for (uint64_t seq = 1; seq <= kQueueLimit; ++seq) {
    CHECK(connection.Enqueue("X", MakeIncrementalMsg("X", seq), true));
  }

  // Control messages must never be silently discarded by the bound.
  mdd::ServerMsg pong;
  pong.mutable_pong()->set_server_timestamp_ns(1);
  CHECK(connection.Enqueue("X", pong, false));
  CHECK(connection.TotalPending() == kQueueLimit + 1);
  return 0;
}

int TestEnqueueResetReplacesQueue() {
  mdd::server::ClientConnection connection("c1", nullptr, kQueueLimit);

  CHECK(connection.Enqueue("X", MakeIncrementalMsg("X", 1), true));
  CHECK(connection.Enqueue("X", MakeIncrementalMsg("X", 2), true));

  mdd::ServerMsg snapshot_msg;
  snapshot_msg.mutable_snapshot()->set_instrument_id("X");
  snapshot_msg.mutable_snapshot()->set_snapshot_seq(50);
  CHECK(connection.EnqueueReset("X", snapshot_msg));
  CHECK(connection.TotalPending() == 1);

  mdd::ServerMsg popped;
  CHECK(connection.PopNext(&popped));
  CHECK(popped.has_snapshot());
  CHECK(popped.snapshot().snapshot_seq() == 50);

  connection.Close();
  CHECK(!connection.PopNext(&popped));
  return 0;
}

int TestPopOrderAndDrain() {
  mdd::server::ClientConnection connection("c1", nullptr, kQueueLimit);
  CHECK(connection.Enqueue("X", MakeIncrementalMsg("X", 1), true));
  CHECK(connection.Enqueue("X", MakeIncrementalMsg("X", 2), true));

  mdd::ServerMsg popped;
  CHECK(connection.PopNext(&popped));
  CHECK(popped.incremental().seq() == 1);
  CHECK(connection.PopNext(&popped));
  CHECK(popped.incremental().seq() == 2);

  // Closed connections drain remaining messages before reporting exhaustion.
  CHECK(connection.Enqueue("X", MakeIncrementalMsg("X", 3), true));
  connection.Close();
  CHECK(!connection.Enqueue("X", MakeIncrementalMsg("X", 4), true));
  CHECK(connection.PopNext(&popped));
  CHECK(popped.incremental().seq() == 3);
  CHECK(!connection.PopNext(&popped));
  return 0;
}

int TestDirtyFlagLifecycle() {
  mdd::server::ClientConnection connection("c1", nullptr, kQueueLimit);
  CHECK(!connection.IsDirty("X"));
  connection.MarkDirty("X");
  CHECK(connection.IsDirty("X"));
  CHECK(!connection.IsDirty("Y"));
  connection.ClearDirty("X");
  CHECK(!connection.IsDirty("X"));
  CHECK(connection.IncrementDropped("X") == 1);
  CHECK(connection.IncrementDropped("X") == 2);
  return 0;
}

int TestPublisherBackpressureDropAndReset() {
  mdd::server::SubscriptionManager subscriptions;
  mdd::server::Publisher publisher(&subscriptions);
  publisher.SetSnapshotProvider(
      [](const std::string& instrument_id, bool is_reset, const std::string& reason) {
        mdd::Snapshot snapshot;
        snapshot.set_instrument_id(instrument_id);
        snapshot.set_snapshot_seq(99);
        snapshot.set_is_reset(is_reset);
        snapshot.set_reason(reason);
        return snapshot;
      });

  subscriptions.AddClient("c1");
  CHECK(subscriptions.Subscribe("c1", "X", 10, 1));
  auto connection = publisher.RegisterClient("c1", nullptr, 2);

  publisher.PublishIncremental(MakeIncremental("X", 1));
  publisher.PublishIncremental(MakeIncremental("X", 2));
  CHECK(connection->TotalPending() == 2);

  // Queue full: the incremental is dropped and the instrument marked dirty.
  publisher.PublishIncremental(MakeIncremental("X", 3));
  CHECK(connection->TotalPending() == 2);
  CHECK(connection->IsDirty("X"));

  // Next publish cycle replaces the stale queue with a reset snapshot.
  publisher.PublishIncremental(MakeIncremental("X", 4));
  CHECK(!connection->IsDirty("X"));
  CHECK(connection->TotalPending() == 1);

  mdd::ServerMsg popped;
  CHECK(connection->PopNext(&popped));
  CHECK(popped.has_snapshot());
  CHECK(popped.snapshot().is_reset());
  CHECK(popped.snapshot().snapshot_seq() == 99);
  CHECK(popped.snapshot().reason() == "BACKPRESSURE_RECOVERY");

  publisher.UnregisterClient("c1");
  return 0;
}

}  // namespace

int main() {
  if (TestIncrementalQueueBound() != 0) return 1;
  if (TestControlBypassesBound() != 0) return 1;
  if (TestEnqueueResetReplacesQueue() != 0) return 1;
  if (TestPopOrderAndDrain() != 0) return 1;
  if (TestDirtyFlagLifecycle() != 0) return 1;
  if (TestPublisherBackpressureDropAndReset() != 0) return 1;

  std::cout << "test_publisher passed\n";
  return 0;
}
