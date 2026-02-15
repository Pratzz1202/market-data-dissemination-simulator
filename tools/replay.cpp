#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/recording.h"
#include "server/order_book.h"

namespace {

struct InstrumentReplayState {
  mdd::server::OrderBook book;
  bool has_seq = false;
  uint64_t last_seq = 0;
  uint64_t snapshots = 0;
  uint64_t incrementals = 0;
  uint64_t gaps = 0;
};

struct CliOptions {
  std::string record_path;
  bool strict = false;
};

bool ParseArgs(int argc, char** argv, CliOptions* options, std::string* error) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--record_path") {
      if (i + 1 >= argc) {
        if (error != nullptr) *error = "missing value for --record_path";
        return false;
      }
      options->record_path = argv[++i];
    } else if (arg == "--strict") {
      options->strict = true;
    } else {
      if (error != nullptr) *error = "unknown arg: " + arg;
      return false;
    }
  }

  if (options->record_path.empty()) {
    if (error != nullptr) {
      *error = "--record_path is required";
    }
    return false;
  }
  return true;
}

void PrintUsage() { std::cerr << "Usage: mdd_replay --record_path <file> [--strict]\n"; }

}  // namespace

int main(int argc, char** argv) {
  CliOptions options;
  std::string parse_error;
  if (!ParseArgs(argc, argv, &options, &parse_error)) {
    if (!parse_error.empty()) {
      std::cerr << "error: " << parse_error << "\n";
    }
    PrintUsage();
    return 1;
  }

  mdd::common::RecordReader reader;
  std::string open_error;
  if (!reader.Open(options.record_path, &open_error)) {
    std::cerr << "error: " << open_error << "\n";
    return 1;
  }

  std::unordered_map<std::string, InstrumentReplayState> states;

  uint64_t total_events = 0;
  uint64_t total_snapshots = 0;
  uint64_t total_incrementals = 0;
  uint64_t total_gaps = 0;

  while (true) {
    const auto maybe_event = reader.Next();
    if (!maybe_event.has_value()) {
      break;
    }
    const auto& event = maybe_event.value();
    total_events++;

    if (event.has_snapshot()) {
      const auto& snapshot = event.snapshot();
      auto& state = states[snapshot.instrument_id()];

      std::vector<mdd::server::Level> bids;
      bids.reserve(snapshot.bids_size());
      for (const auto& level : snapshot.bids()) {
        bids.push_back(mdd::server::Level{level.price(), level.size()});
      }

      std::vector<mdd::server::Level> asks;
      asks.reserve(snapshot.asks_size());
      for (const auto& level : snapshot.asks()) {
        asks.push_back(mdd::server::Level{level.price(), level.size()});
      }

      state.book.Replace(bids, asks);
      state.has_seq = true;
      state.last_seq = snapshot.snapshot_seq();
      state.snapshots++;
      total_snapshots++;
      continue;
    }

    if (event.has_incremental()) {
      const auto& incremental = event.incremental();
      auto& state = states[incremental.instrument_id()];

      if (!state.has_seq || incremental.prev_seq() != state.last_seq) {
        state.gaps++;
        total_gaps++;
      }

      std::vector<mdd::server::BookUpdate> updates;
      updates.reserve(incremental.updates_size());
      for (const auto& delta : incremental.updates()) {
        updates.push_back(mdd::server::FromProto(delta));
      }
      std::string apply_error;
      if (!state.book.ApplyBatch(updates, &apply_error)) {
        std::cerr << "apply error for instrument=" << incremental.instrument_id()
                  << " message=" << apply_error << "\n";
      }

      state.has_seq = true;
      state.last_seq = incremental.seq();
      state.incrementals++;
      total_incrementals++;
    }
  }

  std::cout << "=== mdd_replay summary ===\n";
  std::cout << "record_path=" << options.record_path << "\n";
  std::cout << "total_events=" << total_events << " total_snapshots=" << total_snapshots
            << " total_incrementals=" << total_incrementals
            << " total_gap_detections=" << total_gaps << "\n";

  for (const auto& [instrument, state] : states) {
    std::cout << "instrument=" << instrument << " snapshots=" << state.snapshots
              << " incrementals=" << state.incrementals << " gaps=" << state.gaps
              << " final_seq=" << state.last_seq << "\n";
  }

  if (options.strict && total_gaps > 0) {
    return 2;
  }

  return 0;
}
