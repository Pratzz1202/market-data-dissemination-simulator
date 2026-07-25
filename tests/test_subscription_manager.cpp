#include <algorithm>

#include "server/subscription_manager.h"
#include "tests/test_util.h"

namespace {

int TestSubscribeLifecycle() {
  mdd::server::SubscriptionManager manager;

  // Subscribing an unknown client is rejected.
  CHECK(!manager.Subscribe("ghost", "X", 10, 1));

  manager.AddClient("c1");
  CHECK(manager.ConnectedClients() == 1);
  CHECK(manager.Subscribe("c1", "X", 10, 7));
  CHECK(!manager.Subscribe("c1", "X", 5, 8));  // duplicate rejected
  CHECK(manager.IsSubscribed("c1", "X"));
  CHECK(!manager.IsSubscribed("c1", "Y"));
  CHECK(manager.RequestedDepth("c1", "X") == 10);
  CHECK(manager.SubscriptionId("c1", "X") == 7);

  CHECK(manager.Unsubscribe("c1", "X"));
  CHECK(!manager.Unsubscribe("c1", "X"));
  CHECK(!manager.IsSubscribed("c1", "X"));
  CHECK(manager.SubscribersFor("X").empty());
  return 0;
}

int TestFanoutBookkeeping() {
  mdd::server::SubscriptionManager manager;
  manager.AddClient("c1");
  manager.AddClient("c2");
  CHECK(manager.Subscribe("c1", "X", 10, 1));
  CHECK(manager.Subscribe("c2", "X", 10, 2));
  CHECK(manager.Subscribe("c2", "Y", 10, 3));

  auto subscribers = manager.SubscribersFor("X");
  std::sort(subscribers.begin(), subscribers.end());
  CHECK(subscribers.size() == 2);
  CHECK(subscribers[0] == "c1");
  CHECK(subscribers[1] == "c2");

  auto instruments = manager.SubscriptionsForClient("c2");
  std::sort(instruments.begin(), instruments.end());
  CHECK(instruments.size() == 2);
  CHECK(instruments[0] == "X");
  CHECK(instruments[1] == "Y");

  auto removed = manager.RemoveClient("c2");
  std::sort(removed.begin(), removed.end());
  CHECK(removed.size() == 2);
  CHECK(removed[0] == "X");
  CHECK(removed[1] == "Y");
  CHECK(manager.ConnectedClients() == 1);
  CHECK(manager.SubscribersFor("X").size() == 1);
  CHECK(manager.SubscribersFor("Y").empty());
  return 0;
}

}  // namespace

int main() {
  if (TestSubscribeLifecycle() != 0) return 1;
  if (TestFanoutBookkeeping() != 0) return 1;

  std::cout << "test_subscription_manager passed\n";
  return 0;
}
