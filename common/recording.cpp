#include "common/recording.h"

#include <cstring>

#include "common/logging.h"

namespace mdd::common {

namespace {

bool WriteSizedMessage(std::ostream* out, const google::protobuf::Message& msg) {
  if (out == nullptr) {
    return false;
  }
  const auto size_u64 = msg.ByteSizeLong();
  if (size_u64 > static_cast<size_t>(UINT32_MAX)) {
    return false;
  }

  const uint32_t size = static_cast<uint32_t>(size_u64);
  std::string payload;
  payload.resize(size);
  if (!msg.SerializeToArray(payload.data(), static_cast<int>(payload.size()))) {
    return false;
  }

  out->write(reinterpret_cast<const char*>(&size), sizeof(size));
  out->write(payload.data(), static_cast<std::streamsize>(payload.size()));
  return out->good();
}

bool ReadSizedMessage(std::istream* in, google::protobuf::Message* msg) {
  if (in == nullptr || msg == nullptr) {
    return false;
  }
  uint32_t size = 0;
  in->read(reinterpret_cast<char*>(&size), sizeof(size));
  if (!in->good()) {
    return false;
  }

  std::string payload(size, '\0');
  in->read(payload.data(), static_cast<std::streamsize>(payload.size()));
  if (!in->good()) {
    return false;
  }

  return msg->ParseFromArray(payload.data(), static_cast<int>(payload.size()));
}

}  // namespace

Recorder::Recorder() = default;
Recorder::~Recorder() { Close(); }

bool Recorder::Open(const std::string& path, std::string* error) {
  std::lock_guard<std::mutex> lock(mu_);
  out_.open(path, std::ios::binary | std::ios::trunc);
  if (!out_.is_open()) {
    if (error != nullptr) {
      *error = "failed to open record file: " + path;
    }
    return false;
  }
  enabled_ = true;
  return true;
}

void Recorder::Close() {
  std::lock_guard<std::mutex> lock(mu_);
  enabled_ = false;
  if (out_.is_open()) {
    out_.flush();
    out_.close();
  }
}

bool Recorder::Enabled() const {
  std::lock_guard<std::mutex> lock(mu_);
  return enabled_;
}

bool Recorder::RecordSnapshot(const mdd::Snapshot& snapshot) {
  mdd::RecordedEvent event;
  event.set_record_timestamp_ns(NowNs());
  *event.mutable_snapshot() = snapshot;
  return WriteRecord(event);
}

bool Recorder::RecordIncremental(const mdd::Incremental& incremental) {
  mdd::RecordedEvent event;
  event.set_record_timestamp_ns(NowNs());
  *event.mutable_incremental() = incremental;
  return WriteRecord(event);
}

bool Recorder::WriteRecord(const mdd::RecordedEvent& event) {
  std::lock_guard<std::mutex> lock(mu_);
  if (!enabled_ || !out_.is_open()) {
    return false;
  }
  const bool ok = WriteSizedMessage(&out_, event);
  if (!ok) {
    Logger::Instance().Log("record_write_failed", {});
  }
  return ok;
}

bool RecordReader::Open(const std::string& path, std::string* error) {
  in_.open(path, std::ios::binary);
  if (!in_.is_open()) {
    if (error != nullptr) {
      *error = "failed to open record file: " + path;
    }
    return false;
  }
  return true;
}

void RecordReader::Close() {
  if (in_.is_open()) {
    in_.close();
  }
}

std::optional<mdd::RecordedEvent> RecordReader::Next() {
  if (!in_.is_open()) {
    return std::nullopt;
  }

  mdd::RecordedEvent event;
  if (!ReadSizedMessage(&in_, &event)) {
    return std::nullopt;
  }
  return event;
}

}  // namespace mdd::common
