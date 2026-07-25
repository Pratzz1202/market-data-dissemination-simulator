#pragma once

#include <atomic>
#include <concepts>
#include <cstdint>
#include <initializer_list>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

namespace mdd::common {

// kDebug covers per-update hot-path events (e.g. one line per incremental) and
// is disabled by default so the publish path does not pay formatting costs.
enum class LogLevel { kDebug = 0, kInfo = 1 };

class Logger {
 public:
  static Logger& Instance();

  void SetOutput(std::ostream* out);
  void SetMinLevel(LogLevel level);
  bool DebugEnabled() const;

  void Log(const std::string& event,
           std::initializer_list<std::pair<std::string, std::string>> fields = {});

  // Emitted only when the minimum level is kDebug. Callers with expensive
  // field expressions should additionally guard on DebugEnabled(), since
  // arguments are evaluated before the level check.
  void LogDebug(const std::string& event,
                std::initializer_list<std::pair<std::string, std::string>> fields = {});

 private:
  Logger() = default;

  void Write(const std::string& event,
             std::initializer_list<std::pair<std::string, std::string>> fields);

  std::mutex mu_;
  std::ostream* out_ = nullptr;  // guarded by mu_; nullptr means std::cout
  std::atomic<int> min_level_{static_cast<int>(LogLevel::kInfo)};
};

uint64_t NowNs();

std::string ToString(bool value);
std::string ToString(double value);

// One template covers every integral width. Named overloads per fixed-width
// alias are not portable: uint64_t and size_t alias the same fundamental type
// on LP64 Linux but different ones on macOS.
template <std::integral T>
std::string ToString(T value) {
  return std::to_string(value);
}

}  // namespace mdd::common
