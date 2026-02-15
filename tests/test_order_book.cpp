#include <iostream>
#include <string>

#include "server/order_book.h"

namespace {

#define CHECK(expr)                                                        \
  do {                                                                     \
    if (!(expr)) {                                                         \
      std::cerr << "CHECK failed at line " << __LINE__ << ": " #expr "\n"; \
      return 1;                                                            \
    }                                                                      \
  } while (false)

int TestUpsertAndRemove() {
  mdd::server::OrderBook book;

  std::string error;
  CHECK(book.Apply({mdd::server::Side::kBid, mdd::server::UpdateType::kUpsert, 10000, 10}, &error));
  CHECK(book.Apply({mdd::server::Side::kBid, mdd::server::UpdateType::kUpsert, 9999, 20}, &error));
  CHECK(book.Apply({mdd::server::Side::kAsk, mdd::server::UpdateType::kUpsert, 10001, 30}, &error));

  auto bids = book.TopBids(2);
  auto asks = book.TopAsks(2);

  CHECK(bids.size() == 2);
  CHECK(asks.size() == 1);
  CHECK(bids[0].price == 10000);
  CHECK(bids[1].price == 9999);
  CHECK(asks[0].price == 10001);

  CHECK(book.Apply({mdd::server::Side::kBid, mdd::server::UpdateType::kRemove, 9999, 0}, &error));
  bids = book.TopBids(2);
  CHECK(bids.size() == 1);
  CHECK(bids[0].price == 10000);

  return 0;
}

int TestNegativeSizeRejected() {
  mdd::server::OrderBook book;
  std::string error;
  CHECK(
      !book.Apply({mdd::server::Side::kBid, mdd::server::UpdateType::kUpsert, 10000, -1}, &error));
  CHECK(!error.empty());
  return 0;
}

int TestCrossingDetection() {
  mdd::server::OrderBook book;
  std::string error;
  CHECK(book.Apply({mdd::server::Side::kBid, mdd::server::UpdateType::kUpsert, 10010, 10}, &error));
  CHECK(book.Apply({mdd::server::Side::kAsk, mdd::server::UpdateType::kUpsert, 10005, 10}, &error));

  CHECK(book.IsCrossed());
  CHECK(!book.Validate(false));
  CHECK(book.Validate(true));
  return 0;
}

}  // namespace

int main() {
  if (TestUpsertAndRemove() != 0) return 1;
  if (TestNegativeSizeRejected() != 0) return 1;
  if (TestCrossingDetection() != 0) return 1;

  std::cout << "test_order_book passed\n";
  return 0;
}
