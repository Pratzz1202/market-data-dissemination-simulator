#pragma once

#include <charconv>
#include <string_view>
#include <system_error>

namespace mdd::common {

// Exception-free numeric parsing for CLI flags: std::sto* aborts the process
// on junk input via uncaught std::invalid_argument. Requires the entire
// string to parse.
template <typename T>
bool ParseNumber(const char* text, T* out) {
  if (text == nullptr || out == nullptr) {
    return false;
  }
  const std::string_view view(text);
  if (view.empty()) {
    return false;
  }
  T value{};
  const auto [ptr, ec] = std::from_chars(view.data(), view.data() + view.size(), value);
  if (ec != std::errc() || ptr != view.data() + view.size()) {
    return false;
  }
  *out = value;
  return true;
}

}  // namespace mdd::common
