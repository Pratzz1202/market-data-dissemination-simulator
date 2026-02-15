#pragma once

#include <initializer_list>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

namespace mdd::common {

class Logger {
 public:
  static Logger& Instance();

  void SetOutput(std::ostream* out);

  void Log(const std::string& event,
           std::initializer_list<std::pair<std::string, std::string>> fields = {});

 private:
  Logger() = default;

  std::mutex mu_;
  std::ostream* out_;
};

uint64_t NowNs();

std::string ToString(uint64_t value);
std::string ToString(int64_t value);
std::string ToString(size_t value);
std::string ToString(double value);
std::string ToString(bool value);

}  // namespace mdd::common
