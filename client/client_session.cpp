#include "client/client_session.h"

#include <chrono>
#include <utility>

#include "common/logging.h"
#include "common/metrics.h"

namespace mdd::client {

ClientSession::ClientSession(ClientSessionOptions options) : options_(std::move(options)) {
  if (options_.channel_override != nullptr) {
    channel_ = options_.channel_override;
  } else {
    channel_ = grpc::CreateChannel(options_.target, grpc::InsecureChannelCredentials());
  }
  stub_ = mdd::MarketDataService::NewStub(channel_);
}

ClientSession::~ClientSession() { Stop(); }

void ClientSession::Start() {
  if (started_.exchange(true)) {
    return;
  }
  stop_.store(false);
  run_thread_ = std::thread([this] { Run(); });
}

void ClientSession::Stop() {
  if (!started_.exchange(false)) {
    return;
  }
  stop_.store(true);
  stream_connected_.store(false);
  {
    std::lock_guard<std::mutex> lock(stream_ctx_mu_);
    if (active_context_ != nullptr) {
      active_context_->TryCancel();
    }
  }
  {
    std::lock_guard<std::mutex> lock(queue_mu_);
    queue_cv_.notify_all();
  }
  if (run_thread_.joinable()) {
    run_thread_.join();
  }
}

void ClientSession::SetCallbacks(ClientSessionCallbacks callbacks) {
  std::lock_guard<std::mutex> lock(callbacks_mu_);
  callbacks_ = std::move(callbacks);
}

void ClientSession::Subscribe(const std::string& instrument_id, uint32_t depth,
                              uint64_t subscription_id) {
  if (subscription_id == 0) {
    std::lock_guard<std::mutex> lock(desired_mu_);
    subscription_id = subscription_counter_++;
  }

  {
    std::lock_guard<std::mutex> lock(desired_mu_);
    desired_subscriptions_[instrument_id] = DesiredSubscription{depth, subscription_id};
  }

  apply_engine_.SetSubscribed(instrument_id, depth);

  mdd::ClientMsg msg;
  auto* subscribe = msg.mutable_subscribe();
  subscribe->set_instrument_id(instrument_id);
  subscribe->set_requested_depth(depth);
  subscribe->set_subscription_id(subscription_id);
  if (stream_connected_.load()) {
    Enqueue(msg);
  }

  if (options_.verbose) {
    common::Logger::Instance().Log(
        "subscribe_sent", {{"instrument_id", instrument_id},
                           {"requested_depth", common::ToString(static_cast<uint64_t>(depth))},
                           {"subscription_id", common::ToString(subscription_id)}});
  }
}

void ClientSession::Unsubscribe(const std::string& instrument_id) {
  {
    std::lock_guard<std::mutex> lock(desired_mu_);
    desired_subscriptions_.erase(instrument_id);
  }

  mdd::ClientMsg msg;
  msg.mutable_unsubscribe()->set_instrument_id(instrument_id);
  if (stream_connected_.load()) {
    Enqueue(msg);
  }
}

void ClientSession::Ping() {
  if (!stream_connected_.load()) {
    return;
  }
  mdd::ClientMsg msg;
  msg.mutable_ping()->set_client_timestamp_ns(common::NowNs());
  Enqueue(msg);
}

std::optional<InstrumentState> ClientSession::GetInstrumentState(
    const std::string& instrument_id) const {
  return apply_engine_.GetState(instrument_id);
}

void ClientSession::Enqueue(const mdd::ClientMsg& msg) {
  std::lock_guard<std::mutex> lock(queue_mu_);
  outbound_queue_.push_back(msg);
  queue_cv_.notify_one();
}

bool ClientSession::WriteInitialHandshake(
    grpc::ClientReaderWriter<mdd::ClientMsg, mdd::ServerMsg>* stream) {
  mdd::ClientMsg hello;
  auto* payload = hello.mutable_hello();
  payload->set_schema_version(options_.schema_version);
  payload->set_client_build_id(options_.client_build_id);
  payload->add_capabilities("snapshot_incremental_v1");
  payload->add_capabilities("resync");
  payload->add_capabilities("reset");
  payload->add_capabilities("auto_reconnect");
  return stream->Write(hello);
}

bool ClientSession::WriteCurrentSubscriptions(
    grpc::ClientReaderWriter<mdd::ClientMsg, mdd::ServerMsg>* stream) {
  std::unordered_map<std::string, DesiredSubscription> copy;
  {
    std::lock_guard<std::mutex> lock(desired_mu_);
    copy = desired_subscriptions_;
  }

  for (const auto& [instrument_id, sub] : copy) {
    mdd::ClientMsg msg;
    auto* subscribe = msg.mutable_subscribe();
    subscribe->set_instrument_id(instrument_id);
    subscribe->set_requested_depth(sub.depth);
    subscribe->set_subscription_id(sub.subscription_id);
    if (!stream->Write(msg)) {
      return false;
    }
  }
  return true;
}

void ClientSession::Run() {
  while (!stop_.load()) {
    grpc::ClientContext context;
    {
      std::lock_guard<std::mutex> lock(stream_ctx_mu_);
      active_context_ = &context;
    }
    auto stream = stub_->Stream(&context);
    if (!stream) {
      {
        std::lock_guard<std::mutex> lock(stream_ctx_mu_);
        active_context_ = nullptr;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(options_.reconnect_delay_ms));
      continue;
    }

    if (!WriteInitialHandshake(stream.get()) || !WriteCurrentSubscriptions(stream.get())) {
      stream->WritesDone();
      auto status = stream->Finish();
      {
        std::lock_guard<std::mutex> lock(stream_ctx_mu_);
        active_context_ = nullptr;
      }
      stream_connected_.store(false);
      if (options_.verbose) {
        common::Logger::Instance().Log(
            "connect_failed",
            {{"target", options_.target},
             {"grpc_code", common::ToString(static_cast<int64_t>(status.error_code()))}});
      }
      if (stop_.load() || !options_.auto_reconnect) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(options_.reconnect_delay_ms));
      continue;
    }

    if (options_.verbose) {
      common::Logger::Instance().Log("connected", {{"target", options_.target}});
    }
    stream_connected_.store(true);
    {
      std::lock_guard<std::mutex> lock(callbacks_mu_);
      if (callbacks_.on_connected) {
        callbacks_.on_connected();
      }
    }

    std::atomic<bool> stream_active{true};
    std::thread writer_thread(
        [this, &stream, &stream_active] { WriterLoop(stream.get(), &stream_active); });

    mdd::ServerMsg incoming;
    while (!stop_.load() && stream_active.load() && stream->Read(&incoming)) {
      HandleServerMessage(incoming);
    }

    stream_active.store(false);
    stream_connected_.store(false);
    queue_cv_.notify_all();
    if (writer_thread.joinable()) {
      writer_thread.join();
    }

    {
      std::lock_guard<std::mutex> lock(queue_mu_);
      outbound_queue_.clear();
    }

    stream->WritesDone();
    auto status = stream->Finish();
    {
      std::lock_guard<std::mutex> lock(stream_ctx_mu_);
      active_context_ = nullptr;
    }

    if (options_.verbose) {
      common::Logger::Instance().Log(
          "disconnected",
          {{"target", options_.target},
           {"grpc_code", common::ToString(static_cast<int64_t>(status.error_code()))}});
    }
    {
      std::lock_guard<std::mutex> lock(callbacks_mu_);
      if (callbacks_.on_disconnected) {
        callbacks_.on_disconnected();
      }
    }

    if (stop_.load() || !options_.auto_reconnect) {
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(options_.reconnect_delay_ms));
  }
}

void ClientSession::WriterLoop(grpc::ClientReaderWriter<mdd::ClientMsg, mdd::ServerMsg>* stream,
                               std::atomic<bool>* stream_active) {
  while (!stop_.load() && stream_active->load()) {
    mdd::ClientMsg outgoing;
    {
      std::unique_lock<std::mutex> lock(queue_mu_);
      queue_cv_.wait(
          lock, [&] { return stop_.load() || !stream_active->load() || !outbound_queue_.empty(); });
      if (stop_.load() || !stream_active->load()) {
        break;
      }
      if (outbound_queue_.empty()) {
        continue;
      }
      outgoing = outbound_queue_.front();
      outbound_queue_.pop_front();
    }

    if (!stream->Write(outgoing)) {
      stream_active->store(false);
      break;
    }
  }
}

void ClientSession::RequestResync(const std::string& instrument_id, const std::string& reason,
                                  uint64_t last_seq_seen) {
  if (!apply_engine_.BeginResync(instrument_id)) {
    return;
  }
  if (!stream_connected_.load()) {
    apply_engine_.EndResync(instrument_id);
    return;
  }

  mdd::ClientMsg msg;
  auto* resync = msg.mutable_resync_request();
  resync->set_instrument_id(instrument_id);
  resync->set_reason(reason);
  resync->set_last_seq_seen(last_seq_seen);
  Enqueue(msg);

  if (options_.verbose) {
    common::Logger::Instance().Log("resync_requested",
                                   {{"instrument_id", instrument_id},
                                    {"reason", reason},
                                    {"last_seq_seen", common::ToString(last_seq_seen)}});
  }

  {
    std::lock_guard<std::mutex> lock(callbacks_mu_);
    if (callbacks_.on_resync_requested) {
      callbacks_.on_resync_requested(instrument_id);
    }
  }
}

void ClientSession::HandleServerMessage(const mdd::ServerMsg& msg) {
  switch (msg.payload_case()) {
    case mdd::ServerMsg::kServerHello: {
      if (options_.verbose) {
        common::Logger::Instance().Log(
            "server_hello_received",
            {{"schema_version",
              common::ToString(static_cast<uint64_t>(msg.server_hello().schema_version()))},
             {"server_build_id", msg.server_hello().server_build_id()}});
      }
      break;
    }
    case mdd::ServerMsg::kSnapshot: {
      apply_engine_.OnSnapshot(msg.snapshot());
      if (msg.snapshot().server_timestamp_ns() > 0) {
        const uint64_t latency = common::NowNs() - msg.snapshot().server_timestamp_ns();
        std::lock_guard<std::mutex> lock(callbacks_mu_);
        if (callbacks_.on_snapshot_latency_ns) {
          callbacks_.on_snapshot_latency_ns(latency);
        }
      }
      if (options_.verbose) {
        common::Logger::Instance().Log(
            "snapshot_received", {{"instrument_id", msg.snapshot().instrument_id()},
                                  {"snapshot_seq", common::ToString(msg.snapshot().snapshot_seq())},
                                  {"is_reset", common::ToString(msg.snapshot().is_reset())}});
      }
      std::lock_guard<std::mutex> lock(callbacks_mu_);
      if (callbacks_.on_snapshot) {
        callbacks_.on_snapshot(msg.snapshot());
      }
      break;
    }
    case mdd::ServerMsg::kIncremental: {
      if (options_.incremental_processing_delay_ms > 0) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(options_.incremental_processing_delay_ms));
      }
      const auto result = apply_engine_.OnIncremental(msg.incremental());
      if (result.decision == ApplyDecision::kApplied) {
        if (msg.incremental().server_timestamp_ns() > 0) {
          const uint64_t latency = common::NowNs() - msg.incremental().server_timestamp_ns();
          std::lock_guard<std::mutex> lock(callbacks_mu_);
          if (callbacks_.on_incremental_latency_ns) {
            callbacks_.on_incremental_latency_ns(latency);
          }
          if (callbacks_.on_latency_ns) {
            callbacks_.on_latency_ns(latency);
          }
        }

        {
          std::lock_guard<std::mutex> lock(callbacks_mu_);
          if (callbacks_.on_incremental_msg) {
            callbacks_.on_incremental_msg(msg.incremental());
          }
          if (callbacks_.on_incremental) {
            callbacks_.on_incremental();
          }
        }

        if (options_.verbose) {
          common::Logger::Instance().Log("incremental_applied",
                                         {{"instrument_id", msg.incremental().instrument_id()},
                                          {"seq", common::ToString(result.seq)}});
        }
      } else if (result.decision == ApplyDecision::kNeedResync) {
        if (options_.verbose) {
          common::Logger::Instance().Log("gap_detected",
                                         {{"instrument_id", msg.incremental().instrument_id()},
                                          {"expected_prev", common::ToString(result.expected_prev)},
                                          {"got_prev", common::ToString(result.got_prev)},
                                          {"reason", result.reason}});
        }
        RequestResync(msg.incremental().instrument_id(), result.reason, result.expected_prev);
      }
      break;
    }
    case mdd::ServerMsg::kUnsubscribed: {
      apply_engine_.OnUnsubscribed(msg.unsubscribed().instrument_id());
      if (options_.verbose) {
        common::Logger::Instance().Log("unsubscribed_received",
                                       {{"instrument_id", msg.unsubscribed().instrument_id()},
                                        {"reason", msg.unsubscribed().reason()}});
      }
      break;
    }
    case mdd::ServerMsg::kError: {
      if (options_.verbose) {
        common::Logger::Instance().Log("server_error",
                                       {{"instrument_id", msg.error().instrument_id()},
                                        {"code", msg.error().code()},
                                        {"message", msg.error().message()}});
      }
      std::lock_guard<std::mutex> lock(callbacks_mu_);
      if (callbacks_.on_error) {
        callbacks_.on_error(msg.error().code());
      }
      break;
    }
    case mdd::ServerMsg::kPong: {
      const uint64_t now = common::NowNs();
      const uint64_t rtt = now - msg.pong().client_timestamp_ns();
      if (options_.verbose) {
        common::Logger::Instance().Log(
            "pong_received",
            {{"rtt_ns", common::ToString(rtt)},
             {"server_timestamp_ns", common::ToString(msg.pong().server_timestamp_ns())}});
      }
      break;
    }
    case mdd::ServerMsg::PAYLOAD_NOT_SET:
      break;
  }
}

}  // namespace mdd::client
