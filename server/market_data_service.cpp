#include "server/market_data_service.h"

#include <chrono>
#include <stdexcept>

#include "common/logging.h"
#include "common/metrics.h"

namespace mdd::server {

MarketDataServiceImpl::MarketDataServiceImpl(RuntimeConfig runtime_config, uint64_t seed,
                                             ServiceOptions options)
    : runtime_config_(std::move(runtime_config)),
      options_(std::move(options)),
      simulator_(runtime_config_, seed),
      publisher_(&subscriptions_) {
  publisher_.SetSnapshotProvider(
      [this](const std::string& instrument_id, bool is_reset, const std::string& reason) {
        return simulator_.BuildSnapshot(instrument_id, 0, is_reset, reason);
      });

  if (!options_.record_path.empty()) {
    std::string error;
    if (!recorder_.Open(options_.record_path, &error)) {
      throw std::runtime_error(error);
    }
  }
}

MarketDataServiceImpl::~MarketDataServiceImpl() { StopSimulation(); }

void MarketDataServiceImpl::StartSimulation() {
  const bool was_running = running_.exchange(true);
  if (was_running) {
    return;
  }

  for (const auto& instrument_id : simulator_.InstrumentIds()) {
    simulator_threads_.emplace_back([this, instrument_id] { RunInstrumentLoop(instrument_id); });
  }
}

void MarketDataServiceImpl::StopSimulation() {
  const bool was_running = running_.exchange(false);
  if (!was_running) {
    return;
  }

  for (auto& thread : simulator_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  simulator_threads_.clear();
  recorder_.Close();
}

grpc::Status MarketDataServiceImpl::Stream(
    grpc::ServerContext* /*context*/,
    grpc::ServerReaderWriter<mdd::ServerMsg, mdd::ClientMsg>* stream) {
  const std::string client_id = "client-" + common::ToString(client_counter_.fetch_add(1) + 1);

  subscriptions_.AddClient(client_id);
  common::GlobalMetrics().SetConnectedClients(subscriptions_.ConnectedClients());
  auto connection =
      publisher_.RegisterClient(client_id, stream, options_.queue_limit_per_instrument);

  common::Logger::Instance().Log("client_connected", {{"client_id", client_id}});

  std::thread writer_thread([connection] { connection->WriteLoop(); });

  mdd::ServerMsg hello;
  auto* server_hello = hello.mutable_server_hello();
  server_hello->set_schema_version(1);
  server_hello->set_server_build_id(options_.server_build_id);
  server_hello->add_capabilities("snapshot_incremental_v1");
  server_hello->add_capabilities("resync");
  server_hello->add_capabilities("reset");
  publisher_.SendControl(client_id, hello);

  mdd::ClientMsg incoming;
  while (stream->Read(&incoming)) {
    switch (incoming.payload_case()) {
      case mdd::ClientMsg::kHello: {
        if (incoming.hello().schema_version() > 1) {
          publisher_.SendErrorToClient(client_id, "", "UNSUPPORTED_SCHEMA",
                                       "client schema_version unsupported");
        }
        break;
      }
      case mdd::ClientMsg::kSubscribe:
        HandleSubscribe(client_id, incoming.subscribe());
        break;
      case mdd::ClientMsg::kUnsubscribe:
        HandleUnsubscribe(client_id, incoming.unsubscribe());
        break;
      case mdd::ClientMsg::kResyncRequest:
        HandleResync(client_id, incoming.resync_request());
        break;
      case mdd::ClientMsg::kPing:
        HandlePing(client_id, incoming.ping());
        break;
      case mdd::ClientMsg::PAYLOAD_NOT_SET:
        publisher_.SendErrorToClient(client_id, "", "EMPTY_MESSAGE", "empty client message");
        break;
    }
  }

  connection->Close();
  if (writer_thread.joinable()) {
    writer_thread.join();
  }

  subscriptions_.RemoveClient(client_id);
  common::GlobalMetrics().SetConnectedClients(subscriptions_.ConnectedClients());
  publisher_.UnregisterClient(client_id);
  common::Logger::Instance().Log("client_disconnected",
                                 {{"client_id", client_id}, {"reason", "stream_closed"}});

  return grpc::Status::OK;
}

void MarketDataServiceImpl::HandleSubscribe(const std::string& client_id,
                                            const mdd::Subscribe& subscribe) {
  const std::string& instrument_id = subscribe.instrument_id();
  if (!simulator_.HasInstrument(instrument_id)) {
    publisher_.SendErrorToClient(client_id, instrument_id, "UNKNOWN_INSTRUMENT",
                                 "instrument does not exist");
    return;
  }

  if (!subscriptions_.Subscribe(client_id, instrument_id, subscribe.requested_depth(),
                                subscribe.subscription_id())) {
    publisher_.SendErrorToClient(client_id, instrument_id, "ALREADY_SUBSCRIBED",
                                 "duplicate subscribe rejected");
    return;
  }

  common::Logger::Instance().Log(
      "subscribe", {{"client_id", client_id},
                    {"instrument_id", instrument_id},
                    {"subscription_id", common::ToString(subscribe.subscription_id())}});

  const auto snapshot =
      simulator_.BuildSnapshot(instrument_id, subscribe.requested_depth(), false, "SUBSCRIBE");
  publisher_.SendSnapshotToClient(client_id, snapshot);
  common::Logger::Instance().Log("snapshot_sent",
                                 {{"client_id", client_id},
                                  {"instrument_id", instrument_id},
                                  {"snapshot_seq", common::ToString(snapshot.snapshot_seq())}});
}

void MarketDataServiceImpl::HandleUnsubscribe(const std::string& client_id,
                                              const mdd::Unsubscribe& unsubscribe) {
  const std::string& instrument_id = unsubscribe.instrument_id();
  if (!subscriptions_.Unsubscribe(client_id, instrument_id)) {
    publisher_.SendErrorToClient(client_id, instrument_id, "NOT_SUBSCRIBED",
                                 "cannot unsubscribe a non-subscribed instrument");
    return;
  }

  mdd::ServerMsg msg;
  auto* unsub = msg.mutable_unsubscribed();
  unsub->set_instrument_id(instrument_id);
  unsub->set_reason("CLIENT_REQUEST");
  publisher_.SendControl(client_id, msg);
}

void MarketDataServiceImpl::HandleResync(const std::string& client_id,
                                         const mdd::ResyncRequest& resync_request) {
  const std::string& instrument_id = resync_request.instrument_id();
  if (!simulator_.HasInstrument(instrument_id)) {
    publisher_.SendErrorToClient(client_id, instrument_id, "UNKNOWN_INSTRUMENT",
                                 "resync requested for unknown instrument");
    return;
  }

  const uint32_t depth = subscriptions_.RequestedDepth(client_id, instrument_id);
  const auto snapshot = simulator_.BuildSnapshot(instrument_id, depth, true,
                                                 "CLIENT_RESYNC:" + resync_request.reason());

  publisher_.SendSnapshotToClient(client_id, snapshot);
  common::GlobalMetrics().IncrementResyncs();
  common::Logger::Instance().Log(
      "resync_served", {{"client_id", client_id},
                        {"instrument_id", instrument_id},
                        {"last_seq_seen", common::ToString(resync_request.last_seq_seen())}});
}

void MarketDataServiceImpl::HandlePing(const std::string& client_id, const mdd::Ping& ping) {
  mdd::ServerMsg msg;
  auto* pong = msg.mutable_pong();
  pong->set_client_timestamp_ns(ping.client_timestamp_ns());
  pong->set_server_timestamp_ns(common::NowNs());
  publisher_.SendControl(client_id, msg);
}

void MarketDataServiceImpl::RunInstrumentLoop(const std::string& instrument_id) {
  const uint32_t updates_per_sec = simulator_.UpdatesPerSec(instrument_id);
  const auto period =
      std::chrono::nanoseconds(1000000000ull / std::max<uint32_t>(1, updates_per_sec));
  auto next = std::chrono::steady_clock::now();

  while (running_.load()) {
    next += period;

    try {
      const auto incremental = simulator_.GenerateIncremental(instrument_id);
      if (options_.drop_every_n > 0 && incremental.seq() % options_.drop_every_n == 0) {
        const auto subscribers = subscriptions_.SubscribersFor(instrument_id);
        const uint64_t dropped_subscribers = static_cast<uint64_t>(subscribers.size());
        if (dropped_subscribers > 0) {
          common::GlobalMetrics().IncrementLossSimulatedDrops(dropped_subscribers);
        }
        common::Logger::Instance().Log(
            "loss_simulation_drop",
            {{"instrument_id", instrument_id},
             {"seq", common::ToString(incremental.seq())},
             {"dropped_subscribers", common::ToString(dropped_subscribers)}});
      } else {
        publisher_.PublishIncremental(incremental);
        if (recorder_.Enabled()) {
          recorder_.RecordIncremental(incremental);
        }

        common::Logger::Instance().Log(
            "incremental_sent",
            {{"instrument_id", instrument_id},
             {"seq", common::ToString(incremental.seq())},
             {"client_count",
              common::ToString(subscriptions_.SubscribersFor(instrument_id).size())}});
      }

      if (simulator_.ShouldEmitReset(instrument_id)) {
        const auto reset_snapshot =
            simulator_.BuildSnapshot(instrument_id, 0, true, "SIMULATED_RESET");
        publisher_.PublishSnapshotToSubscribers(instrument_id, reset_snapshot);
        if (recorder_.Enabled()) {
          recorder_.RecordSnapshot(reset_snapshot);
        }
      }
    } catch (const std::exception& ex) {
      common::Logger::Instance().Log("simulation_error",
                                     {{"instrument_id", instrument_id}, {"message", ex.what()}});
    }

    std::this_thread::sleep_until(next);
  }
}

}  // namespace mdd::server
