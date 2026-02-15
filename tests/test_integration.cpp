#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>

#include "client/client_session.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "server/market_data_service.h"

namespace {

#define CHECK(expr)                                                        \
  do {                                                                     \
    if (!(expr)) {                                                         \
      std::cerr << "CHECK failed at line " << __LINE__ << ": " #expr "\n"; \
      return 1;                                                            \
    }                                                                      \
  } while (false)

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout,
               std::chrono::milliseconds step = std::chrono::milliseconds(20)) {
  const auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < timeout) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(step);
  }
  return predicate();
}

mdd::server::RuntimeConfig BuildConfig(uint32_t updates_per_sec) {
  mdd::server::RuntimeConfig config;
  config.default_updates_per_sec = updates_per_sec;
  config.reset_probability_ppm = 0;
  config.allow_crossed_books = false;

  mdd::server::InstrumentRuntimeConfig instrument;
  instrument.instrument_id = "BTC-USD";
  instrument.symbol = "BTCUSD";
  instrument.publish_depth = 10;
  instrument.tick_size = 1;
  instrument.base_price = 10000;
  instrument.levels_per_side = 30;
  instrument.updates_per_sec = updates_per_sec;
  instrument.volatility = 0.05;
  config.instruments.push_back(instrument);
  return config;
}

class InProcessServerHarness {
 public:
  InProcessServerHarness(const mdd::server::RuntimeConfig& config, uint64_t seed,
                         const mdd::server::ServiceOptions& options) {
    service_ = std::make_unique<mdd::server::MarketDataServiceImpl>(config, seed, options);

    grpc::ServerBuilder builder;
    builder.RegisterService(service_.get());
    server_ = builder.BuildAndStart();
    if (!server_) {
      throw std::runtime_error("failed to start in-process server");
    }

    service_->StartSimulation();

    grpc::ChannelArguments args;
    channel_ = server_->InProcessChannel(args);
    if (!channel_) {
      throw std::runtime_error("failed to create in-process channel");
    }
  }

  ~InProcessServerHarness() {
    if (service_ != nullptr) {
      service_->StopSimulation();
    }
    if (server_ != nullptr) {
      server_->Shutdown();
    }
  }

  std::shared_ptr<grpc::Channel> Channel() const { return channel_; }

 private:
  std::unique_ptr<mdd::server::MarketDataServiceImpl> service_;
  std::unique_ptr<grpc::Server> server_;
  std::shared_ptr<grpc::Channel> channel_;
};

std::unique_ptr<mdd::client::ClientSession> MakeClient(
    const std::shared_ptr<grpc::Channel>& channel, bool verbose, uint32_t processing_delay_ms = 0) {
  mdd::client::ClientSessionOptions options;
  options.target = "inprocess";
  options.auto_reconnect = false;
  options.reconnect_delay_ms = 100;
  options.verbose = verbose;
  options.channel_override = channel;
  options.incremental_processing_delay_ms = processing_delay_ms;
  return std::make_unique<mdd::client::ClientSession>(options);
}

int TestSubscribeSnapshotIncrementalOverGrpc() {
  mdd::server::ServiceOptions service_options;
  InProcessServerHarness harness(BuildConfig(1200), 42, service_options);

  auto client = MakeClient(harness.Channel(), false);

  std::atomic<uint64_t> applied{0};
  std::atomic<uint64_t> errors{0};
  mdd::client::ClientSessionCallbacks callbacks;
  callbacks.on_incremental = [&] { applied.fetch_add(1); };
  callbacks.on_error = [&](const std::string&) { errors.fetch_add(1); };
  client->SetCallbacks(std::move(callbacks));

  client->Start();
  client->Subscribe("BTC-USD", 10);

  const bool ok = WaitUntil(
      [&] {
        const auto state = client->GetInstrumentState("BTC-USD");
        return state.has_value() && state->has_seq && state->last_seq >= 25 && applied.load() >= 10;
      },
      std::chrono::milliseconds(4000));

  client->Stop();

  CHECK(ok);
  CHECK(errors.load() == 0);

  const auto state = client->GetInstrumentState("BTC-USD");
  CHECK(state.has_value());
  CHECK(state->book.Validate());

  return 0;
}

int TestGapInjectionAndResyncOverGrpc() {
  mdd::server::ServiceOptions service_options;
  service_options.drop_every_n = 5;
  InProcessServerHarness harness(BuildConfig(1200), 99, service_options);

  auto client = MakeClient(harness.Channel(), false);

  std::atomic<uint64_t> resyncs{0};
  mdd::client::ClientSessionCallbacks callbacks;
  callbacks.on_resync_requested = [&](const std::string&) { resyncs.fetch_add(1); };
  client->SetCallbacks(std::move(callbacks));

  client->Start();
  client->Subscribe("BTC-USD", 10);

  const bool ok = WaitUntil(
      [&] {
        const auto state = client->GetInstrumentState("BTC-USD");
        return resyncs.load() > 0 && state.has_value() && state->has_seq && state->last_seq > 0;
      },
      std::chrono::milliseconds(5000));

  client->Stop();

  CHECK(ok);
  CHECK(resyncs.load() > 0);

  return 0;
}

int TestBackpressureDropAndResetOverGrpc() {
  mdd::server::ServiceOptions service_options;
  service_options.queue_limit_per_instrument = 4;
  InProcessServerHarness harness(BuildConfig(4000), 123, service_options);

  auto client = MakeClient(harness.Channel(), false, 5);

  const uint64_t drops_before = mdd::common::GlobalMetrics().Snapshot().total_drops;
  std::atomic<uint64_t> reset_snapshots{0};

  mdd::client::ClientSessionCallbacks callbacks;
  callbacks.on_snapshot = [&](const mdd::Snapshot& snapshot) {
    if (snapshot.is_reset() &&
        snapshot.reason().find("BACKPRESSURE_RECOVERY") != std::string::npos) {
      reset_snapshots.fetch_add(1);
    }
  };
  client->SetCallbacks(std::move(callbacks));

  client->Start();
  client->Subscribe("BTC-USD", 10);

  const bool ok =
      WaitUntil([&] { return reset_snapshots.load() > 0; }, std::chrono::milliseconds(6000));

  const uint64_t drops_after = mdd::common::GlobalMetrics().Snapshot().total_drops;

  client->Stop();

  CHECK(ok);
  CHECK(drops_after > drops_before);

  return 0;
}

int TestFanoutConsistencyOverGrpc() {
  mdd::server::ServiceOptions service_options;
  InProcessServerHarness harness(BuildConfig(1000), 777, service_options);

  auto c1 = MakeClient(harness.Channel(), false);
  auto c2 = MakeClient(harness.Channel(), false);

  std::mutex seq_mu_1;
  std::mutex seq_mu_2;
  std::vector<uint64_t> seqs_1;
  std::vector<uint64_t> seqs_2;

  mdd::client::ClientSessionCallbacks cb1;
  cb1.on_incremental_msg = [&](const mdd::Incremental& incremental) {
    std::lock_guard<std::mutex> lock(seq_mu_1);
    seqs_1.push_back(incremental.seq());
  };
  c1->SetCallbacks(std::move(cb1));

  mdd::client::ClientSessionCallbacks cb2;
  cb2.on_incremental_msg = [&](const mdd::Incremental& incremental) {
    std::lock_guard<std::mutex> lock(seq_mu_2);
    seqs_2.push_back(incremental.seq());
  };
  c2->SetCallbacks(std::move(cb2));

  c1->Start();
  c2->Start();
  c1->Subscribe("BTC-USD", 10);
  c2->Subscribe("BTC-USD", 10);

  const bool enough_messages = WaitUntil(
      [&] {
        std::lock_guard<std::mutex> lock1(seq_mu_1);
        std::lock_guard<std::mutex> lock2(seq_mu_2);
        return seqs_1.size() >= 80 && seqs_2.size() >= 80;
      },
      std::chrono::milliseconds(5000));

  c1->Stop();
  c2->Stop();

  CHECK(enough_messages);

  std::vector<uint64_t> left;
  std::vector<uint64_t> right;
  {
    std::lock_guard<std::mutex> lock(seq_mu_1);
    left = seqs_1;
  }
  {
    std::lock_guard<std::mutex> lock(seq_mu_2);
    right = seqs_2;
  }

  const size_t compare_count = std::min(left.size(), right.size());
  CHECK(compare_count >= 80);
  for (size_t i = 0; i < compare_count; ++i) {
    CHECK(left[i] == right[i]);
    if (i > 0) {
      CHECK(left[i] > left[i - 1]);
      CHECK(right[i] > right[i - 1]);
    }
  }

  return 0;
}

}  // namespace

int main() {
  std::ofstream null_log("/dev/null");
  mdd::common::Logger::Instance().SetOutput(&null_log);

  if (TestSubscribeSnapshotIncrementalOverGrpc() != 0) return 1;
  if (TestGapInjectionAndResyncOverGrpc() != 0) return 1;
  if (TestBackpressureDropAndResetOverGrpc() != 0) return 1;
  if (TestFanoutConsistencyOverGrpc() != 0) return 1;

  std::cout << "test_integration passed\n";
  return 0;
}
