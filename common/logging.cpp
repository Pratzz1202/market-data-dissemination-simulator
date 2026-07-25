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
  for (const char c : value) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          std::ostringstream oss;
          oss << "\\u" << std::hex << std::setw(4) << std::setfill('0')
              << static_cast<int>(static_cast<unsigned char>(c));
          out += oss.str();
        } else {
          out.push_back(c);
        }
        break;
    }
  }
  return out;
}

}  // namespace

Logger& Logger::Instance() {
  static Logger logger;
  return logger;
}

void Logger::SetOutput(std::ostream* out) {
  std::lock_guard<std::mutex> lock(mu_);
  out_ = out;
}

void Logger::SetMinLevel(LogLevel level) {
  min_level_.store(static_cast<int>(level), std::memory_order_relaxed);
}

bool Logger::DebugEnabled() const {
  return min_level_.load(std::memory_order_relaxed) <= static_cast<int>(LogLevel::kDebug);
}

void Logger::Log(const std::string& event,
                 std::initializer_list<std::pair<std::string, std::string>> fields) {
  Write(event, fields);
}

void Logger::LogDebug(const std::string& event,
                      std::initializer_list<std::pair<std::string, std::string>> fields) {
  if (!DebugEnabled()) {
    return;
  }
  Write(event, fields);
}

void Logger::Write(const std::string& event,
                   std::initializer_list<std::pair<std::string, std::string>> fields) {
  std::lock_guard<std::mutex> lock(mu_);
  std::ostream* out = out_ == nullptr ? &std::cout : out_;
  (*out) << "{\"ts_ns\":" << NowNs() << ",\"event\":\"" << Escape(event) << "\"";
  for (const auto& [key, value] : fields) {
    (*out) << ",\"" << Escape(key) << "\":\"" << Escape(value) << "\"";
  }
  (*out) << "}\n";
  out->flush();
}

uint64_t NowNs() {
  const auto now =
      std::chrono::time_point_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now());
  return static_cast<uint64_t>(now.time_since_epoch().count());
}

std::string ToString(bool value) { return value ? "true" : "false"; }

std::string ToString(double value) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(6) << value;
  return oss.str();
}

}  // namespace mdd::common
