#include <atomic>
#include <csignal>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "client/client_session.h"
#include "common/logging.h"

namespace {

std::atomic<bool> g_stop{false};

void HandleSignal(int /*signal*/) { g_stop.store(true); }

struct CliOptions {
  std::string host = "localhost:50051";
  std::vector<std::string> instruments;
  uint32_t depth = 10;
  bool auto_reconnect = true;
  uint32_t reconnect_delay_ms = 1000;
  uint32_t ping_interval_ms = 0;
  uint32_t duration_sec = 0;
  bool interactive = false;
};

bool ParseArgs(int argc, char** argv, CliOptions* options, std::string* error) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto next = [&]() -> const char* {
      if (i + 1 >= argc) {
        if (error != nullptr) {
          *error = "missing value for " + arg;
        }
        return nullptr;
      }
      return argv[++i];
    };

    if (arg == "--host") {
      const char* value = next();
      if (value == nullptr) return false;
      options->host = value;
    } else if (arg == "--subscribe") {
      const char* value = next();
      if (value == nullptr) return false;
      options->instruments.push_back(value);
    } else if (arg == "--depth") {
      const char* value = next();
      if (value == nullptr) return false;
      options->depth = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--no_reconnect") {
      options->auto_reconnect = false;
    } else if (arg == "--reconnect_delay_ms") {
      const char* value = next();
      if (value == nullptr) return false;
      options->reconnect_delay_ms = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--ping_interval_ms") {
      const char* value = next();
      if (value == nullptr) return false;
      options->ping_interval_ms = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--duration_sec") {
      const char* value = next();
      if (value == nullptr) return false;
      options->duration_sec = static_cast<uint32_t>(std::stoul(value));
    } else if (arg == "--interactive") {
      options->interactive = true;
    } else {
      if (error != nullptr) {
        *error = "unknown arg: " + arg;
      }
      return false;
    }
  }

  if (options->instruments.empty() && !options->interactive) {
    if (error != nullptr) {
      *error = "at least one --subscribe instrument is required unless --interactive is set";
    }
    return false;
  }
  return true;
}

void PrintUsage() {
  std::cerr << "Usage: mdd_client --host <host:port> [options]\n"
            << "  --subscribe <instrument>   (repeatable)\n"
            << "  --depth <n>\n"
            << "  --duration_sec <n>\n"
            << "  --ping_interval_ms <n>\n"
            << "  --interactive\n"
            << "  --no_reconnect\n";
}

void PrintInteractiveHelp() {
  std::cout << "Interactive commands:\n"
            << "  sub <instrument_id> [depth]\n"
            << "  unsub <instrument_id>\n"
            << "  ping\n"
            << "  show <instrument_id>\n"
            << "  help\n"
            << "  quit\n";
}

}  // namespace

int main(int argc, char** argv) {
  CliOptions options;
  std::string error;
  if (!ParseArgs(argc, argv, &options, &error)) {
    if (!error.empty()) {
      std::cerr << "error: " << error << "\n";
    }
    PrintUsage();
    return 1;
  }

  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);

  mdd::client::ClientSessionOptions session_options;
  session_options.target = options.host;
  session_options.auto_reconnect = options.auto_reconnect;
  session_options.reconnect_delay_ms = options.reconnect_delay_ms;

  mdd::client::ClientSession session(session_options);
  session.Start();

  std::unordered_set<std::string> tracked_instruments;
  std::mutex tracked_mu;
  for (const auto& instrument : options.instruments) {
    session.Subscribe(instrument, options.depth);
    tracked_instruments.insert(instrument);
  }

  std::thread command_thread;
  if (options.interactive && options.duration_sec == 0) {
    PrintInteractiveHelp();
    command_thread = std::thread([&] {
      std::string line;
      while (!g_stop.load() && std::getline(std::cin, line)) {
        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;
        if (cmd.empty()) {
          continue;
        }

        if (cmd == "sub") {
          std::string instrument;
          uint32_t depth = options.depth;
          iss >> instrument;
          if (instrument.empty()) {
            std::cout << "usage: sub <instrument_id> [depth]\n";
            continue;
          }
          if (!(iss >> depth)) {
            depth = options.depth;
          }
          session.Subscribe(instrument, depth);
          std::lock_guard<std::mutex> lock(tracked_mu);
          tracked_instruments.insert(instrument);
        } else if (cmd == "unsub") {
          std::string instrument;
          iss >> instrument;
          if (instrument.empty()) {
            std::cout << "usage: unsub <instrument_id>\n";
            continue;
          }
          session.Unsubscribe(instrument);
        } else if (cmd == "ping") {
          session.Ping();
        } else if (cmd == "show") {
          std::string instrument;
          iss >> instrument;
          if (instrument.empty()) {
            std::cout << "usage: show <instrument_id>\n";
            continue;
          }
          const auto state = session.GetInstrumentState(instrument);
          if (!state.has_value()) {
            std::cout << "state not found for " << instrument << "\n";
            continue;
          }
          std::cout << "instrument=" << instrument
                    << " subscribed=" << (state->subscribed ? "true" : "false")
                    << " has_seq=" << (state->has_seq ? "true" : "false")
                    << " last_seq=" << state->last_seq << "\n";
        } else if (cmd == "help") {
          PrintInteractiveHelp();
        } else if (cmd == "quit") {
          g_stop.store(true);
          break;
        } else {
          std::cout << "unknown command: " << cmd << "\n";
        }
      }
      g_stop.store(true);
    });
  }

  const auto start = std::chrono::steady_clock::now();
  auto last_ping = start;

  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (options.duration_sec > 0) {
      const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now() - start);
      if (elapsed.count() >= options.duration_sec) {
        break;
      }
    }

    if (options.ping_interval_ms > 0) {
      const auto now = std::chrono::steady_clock::now();
      const auto elapsed =
          std::chrono::duration_cast<std::chrono::milliseconds>(now - last_ping).count();
      if (elapsed >= options.ping_interval_ms) {
        session.Ping();
        last_ping = now;
      }
    }
  }

  if (command_thread.joinable()) {
    command_thread.join();
  }

  std::vector<std::string> instruments_for_dump;
  {
    std::lock_guard<std::mutex> lock(tracked_mu);
    instruments_for_dump.assign(tracked_instruments.begin(), tracked_instruments.end());
  }

  for (const auto& instrument : instruments_for_dump) {
    const auto state = session.GetInstrumentState(instrument);
    if (!state.has_value()) {
      continue;
    }
    mdd::common::Logger::Instance().Log("final_state",
                                        {{"instrument_id", instrument},
                                         {"subscribed", mdd::common::ToString(state->subscribed)},
                                         {"has_seq", mdd::common::ToString(state->has_seq)},
                                         {"last_seq", mdd::common::ToString(state->last_seq)}});
  }

  session.Stop();
  return 0;
}
