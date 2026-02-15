#include "client/order_book_view.h"

namespace mdd::client {

void OrderBookView::Clear() {
  bids_.clear();
  asks_.clear();
}

void OrderBookView::ApplySnapshot(const mdd::Snapshot& snapshot) {
  bids_.clear();
  asks_.clear();

  for (const auto& level : snapshot.bids()) {
    if (level.size() > 0) {
      bids_[level.price()] = level.size();
    }
  }

  for (const auto& level : snapshot.asks()) {
    if (level.size() > 0) {
      asks_[level.price()] = level.size();
    }
  }
}

bool OrderBookView::ApplyIncremental(const mdd::Incremental& incremental, std::string* error) {
  auto fail = [error](const std::string& msg) {
    if (error != nullptr) {
      *error = msg;
    }
    return false;
  };

  for (const auto& update : incremental.updates()) {
    if (update.side() == mdd::SIDE_BID) {
      switch (update.op()) {
        case mdd::DELTA_OP_UPSERT:
          if (update.size() <= 0) {
            return fail("upsert update has non-positive size");
          }
          bids_[update.price()] = update.size();
          break;
        case mdd::DELTA_OP_REMOVE:
          bids_.erase(update.price());
          break;
        case mdd::DELTA_OP_CLEAR:
          bids_.clear();
          break;
        default:
          return fail("unknown delta op");
      }
    } else {
      switch (update.op()) {
        case mdd::DELTA_OP_UPSERT:
          if (update.size() <= 0) {
            return fail("upsert update has non-positive size");
          }
          asks_[update.price()] = update.size();
          break;
        case mdd::DELTA_OP_REMOVE:
          asks_.erase(update.price());
          break;
        case mdd::DELTA_OP_CLEAR:
          asks_.clear();
          break;
        default:
          return fail("unknown delta op");
      }
    }
  }

  return true;
}

std::vector<PriceSize> OrderBookView::TopBids(size_t depth) const {
  std::vector<PriceSize> out;
  out.reserve(depth);
  size_t count = 0;
  for (const auto& [price, size] : bids_) {
    out.push_back(PriceSize{price, size});
    if (++count >= depth) break;
  }
  return out;
}

std::vector<PriceSize> OrderBookView::TopAsks(size_t depth) const {
  std::vector<PriceSize> out;
  out.reserve(depth);
  size_t count = 0;
  for (const auto& [price, size] : asks_) {
    out.push_back(PriceSize{price, size});
    if (++count >= depth) break;
  }
  return out;
}

bool OrderBookView::IsCrossed() const {
  if (bids_.empty() || asks_.empty()) {
    return false;
  }
  return bids_.begin()->first >= asks_.begin()->first;
}

bool OrderBookView::Validate() const {
  for (const auto& [_, size] : bids_) {
    if (size <= 0) return false;
  }
  for (const auto& [_, size] : asks_) {
    if (size <= 0) return false;
  }
  return !IsCrossed();
}

}  // namespace mdd::client
