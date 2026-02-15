#include "common/logging.h"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace mdd::common {

namespace {

std::string Escape(const std::string& value) {
  std::string out;
  out.reserve(value.size());
  for (char c : value) {
    if (c == '"' || c == '\\') {
      out.push_back('\\');
    }
    out.push_back(c);
  }
  return out;
}

}  // namespace

Logger& Logger::Instance() {
  static Logger logger;
  if (logger.out_ == nullptr) {
    logger.out_ = &std::cout;
  }
  return logger;
}

void Logger::SetOutput(std::ostream* out) {
  std::lock_guard<std::mutex> lock(mu_);
  out_ = out == nullptr ? &std::cout : out;
}

void Logger::Log(const std::string& event,
                 std::initializer_list<std::pair<std::string, std::string>> fields) {
  std::lock_guard<std::mutex> lock(mu_);
  if (out_ == nullptr) {
    out_ = &std::cout;
  }
  (*out_) << "{\"ts_ns\":" << NowNs() << ",\"event\":\"" << Escape(event) << "\"";
  for (const auto& [key, value] : fields) {
    (*out_) << ",\"" << Escape(key) << "\":\"" << Escape(value) << "\"";
  }
  (*out_) << "}\n";
  out_->flush();
}

uint64_t NowNs() {
  const auto now =
      std::chrono::time_point_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now());
  return static_cast<uint64_t>(now.time_since_epoch().count());
}

// std::string ToString(uint64_t value) { return std::to_string(value); }
std::string ToString(int64_t value) { return std::to_string(value); }
std::string ToString(size_t value) { return std::to_string(value); }

std::string ToString(double value) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(6) << value;
  return oss.str();
}

std::string ToString(bool value) { return value ? "true" : "false"; }

}  // namespace mdd::common
