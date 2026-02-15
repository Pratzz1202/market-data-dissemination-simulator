#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <optional>
#include <string>

#include "mdd.pb.h"

namespace mdd::common {

class Recorder {
 public:
  Recorder();
  ~Recorder();

  bool Open(const std::string& path, std::string* error);
  void Close();

  bool Enabled() const;

  bool RecordSnapshot(const mdd::Snapshot& snapshot);
  bool RecordIncremental(const mdd::Incremental& incremental);

 private:
  bool WriteRecord(const mdd::RecordedEvent& event);

  mutable std::mutex mu_;
  std::ofstream out_;
  bool enabled_ = false;
};

class RecordReader {
 public:
  bool Open(const std::string& path, std::string* error);
  void Close();

  std::optional<mdd::RecordedEvent> Next();

 private:
  std::ifstream in_;
};

}  // namespace mdd::common
