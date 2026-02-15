#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "mdd.pb.h"

namespace mdd::client {

struct PriceSize {
  int64_t price = 0;
  int64_t size = 0;
};

class OrderBookView {
 public:
  void Clear();
  void ApplySnapshot(const mdd::Snapshot& snapshot);
  bool ApplyIncremental(const mdd::Incremental& incremental, std::string* error);

  std::vector<PriceSize> TopBids(size_t depth) const;
  std::vector<PriceSize> TopAsks(size_t depth) const;

  bool IsCrossed() const;
  bool Validate() const;

 private:
  std::map<int64_t, int64_t, std::greater<int64_t>> bids_;
  std::map<int64_t, int64_t, std::less<int64_t>> asks_;
};

}  // namespace mdd::client
