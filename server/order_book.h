#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "mdd.pb.h"

namespace mdd::server {

enum class Side { kBid, kAsk };
enum class UpdateType { kUpsert, kRemove, kClear };

struct Level {
  int64_t price = 0;
  int64_t size = 0;
};

struct BookUpdate {
  Side side = Side::kBid;
  UpdateType type = UpdateType::kUpsert;
  int64_t price = 0;
  int64_t size = 0;
};

class OrderBook {
 public:
  bool Apply(const BookUpdate& update, std::string* error = nullptr);
  bool ApplyBatch(const std::vector<BookUpdate>& updates, std::string* error = nullptr);

  void Clear();
  void ClearSide(Side side);
  void Replace(const std::vector<Level>& bids, const std::vector<Level>& asks);

  std::vector<Level> TopBids(size_t depth) const;
  std::vector<Level> TopAsks(size_t depth) const;

  std::optional<int64_t> BestBid() const;
  std::optional<int64_t> BestAsk() const;

  bool IsCrossed() const;
  bool Validate(bool allow_crossed_books) const;

  size_t BidLevels() const;
  size_t AskLevels() const;

  const std::map<int64_t, int64_t, std::greater<int64_t>>& Bids() const;
  const std::map<int64_t, int64_t, std::less<int64_t>>& Asks() const;

 private:
  std::map<int64_t, int64_t, std::greater<int64_t>> bids_;
  std::map<int64_t, int64_t, std::less<int64_t>> asks_;
};

BookUpdate FromProto(const mdd::LevelDelta& delta);
mdd::LevelDelta ToProto(const BookUpdate& update);

mdd::Side ToProtoSide(Side side);
Side FromProtoSide(mdd::Side side);

}  // namespace mdd::server
