#include "server/order_book.h"

namespace mdd::server {

bool OrderBook::Apply(const BookUpdate& update, std::string* error) {
  auto fail = [error](const std::string& msg) {
    if (error != nullptr) {
      *error = msg;
    }
    return false;
  };

  auto apply_on_side = [&](auto& side_map) {
    switch (update.type) {
      case UpdateType::kUpsert:
        if (update.size <= 0) {
          return fail("upsert size must be > 0");
        }
        side_map[update.price] = update.size;
        return true;
      case UpdateType::kRemove:
        side_map.erase(update.price);
        return true;
      case UpdateType::kClear:
        side_map.clear();
        return true;
      default:
        return fail("unknown update type");
    }
  };

  if (update.price < 0) {
    return fail("price must be >= 0");
  }

  if (update.side == Side::kBid) {
    return apply_on_side(bids_);
  }
  return apply_on_side(asks_);
}

bool OrderBook::ApplyBatch(const std::vector<BookUpdate>& updates, std::string* error) {
  for (const auto& update : updates) {
    if (!Apply(update, error)) {
      return false;
    }
  }
  return true;
}

void OrderBook::Clear() {
  bids_.clear();
  asks_.clear();
}

void OrderBook::ClearSide(Side side) {
  if (side == Side::kBid) {
    bids_.clear();
  } else {
    asks_.clear();
  }
}

void OrderBook::Replace(const std::vector<Level>& bids, const std::vector<Level>& asks) {
  bids_.clear();
  asks_.clear();
  for (const auto& level : bids) {
    if (level.size > 0) {
      bids_[level.price] = level.size;
    }
  }
  for (const auto& level : asks) {
    if (level.size > 0) {
      asks_[level.price] = level.size;
    }
  }
}

std::vector<Level> OrderBook::TopBids(size_t depth) const {
  std::vector<Level> levels;
  levels.reserve(depth);
  size_t count = 0;
  for (const auto& [price, size] : bids_) {
    levels.push_back(Level{price, size});
    if (++count >= depth) {
      break;
    }
  }
  return levels;
}

std::vector<Level> OrderBook::TopAsks(size_t depth) const {
  std::vector<Level> levels;
  levels.reserve(depth);
  size_t count = 0;
  for (const auto& [price, size] : asks_) {
    levels.push_back(Level{price, size});
    if (++count >= depth) {
      break;
    }
  }
  return levels;
}

std::optional<int64_t> OrderBook::BestBid() const {
  if (bids_.empty()) {
    return std::nullopt;
  }
  return bids_.begin()->first;
}

std::optional<int64_t> OrderBook::BestAsk() const {
  if (asks_.empty()) {
    return std::nullopt;
  }
  return asks_.begin()->first;
}

bool OrderBook::IsCrossed() const {
  if (bids_.empty() || asks_.empty()) {
    return false;
  }
  return bids_.begin()->first >= asks_.begin()->first;
}

bool OrderBook::Validate(bool allow_crossed_books) const {
  for (const auto& [_, size] : bids_) {
    if (size <= 0) {
      return false;
    }
  }
  for (const auto& [_, size] : asks_) {
    if (size <= 0) {
      return false;
    }
  }
  if (!allow_crossed_books && IsCrossed()) {
    return false;
  }
  return true;
}

size_t OrderBook::BidLevels() const { return bids_.size(); }
size_t OrderBook::AskLevels() const { return asks_.size(); }

const std::map<int64_t, int64_t, std::greater<int64_t>>& OrderBook::Bids() const { return bids_; }

const std::map<int64_t, int64_t, std::less<int64_t>>& OrderBook::Asks() const { return asks_; }

BookUpdate FromProto(const mdd::LevelDelta& delta) {
  BookUpdate update;
  update.side = FromProtoSide(delta.side());
  switch (delta.op()) {
    case mdd::DELTA_OP_UPSERT:
      update.type = UpdateType::kUpsert;
      break;
    case mdd::DELTA_OP_REMOVE:
      update.type = UpdateType::kRemove;
      break;
    case mdd::DELTA_OP_CLEAR:
      update.type = UpdateType::kClear;
      break;
    default:
      update.type = UpdateType::kUpsert;
      break;
  }
  update.price = delta.price();
  update.size = delta.size();
  return update;
}

mdd::LevelDelta ToProto(const BookUpdate& update) {
  mdd::LevelDelta delta;
  delta.set_side(ToProtoSide(update.side));
  switch (update.type) {
    case UpdateType::kUpsert:
      delta.set_op(mdd::DELTA_OP_UPSERT);
      break;
    case UpdateType::kRemove:
      delta.set_op(mdd::DELTA_OP_REMOVE);
      break;
    case UpdateType::kClear:
      delta.set_op(mdd::DELTA_OP_CLEAR);
      break;
  }
  delta.set_price(update.price);
  delta.set_size(update.size);
  return delta;
}

mdd::Side ToProtoSide(Side side) { return side == Side::kBid ? mdd::SIDE_BID : mdd::SIDE_ASK; }

Side FromProtoSide(mdd::Side side) { return side == mdd::SIDE_ASK ? Side::kAsk : Side::kBid; }

}  // namespace mdd::server
