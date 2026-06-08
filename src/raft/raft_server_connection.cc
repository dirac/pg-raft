#include "src/raft/raft_server_connection.h"

Logger RaftServerConnection::logger{"raft-server-conn"};

Result<ZkResponse> RaftServerConnection::exec(ZkRequest const& request) {
  if (!is_leader()) {
    return ZkResponse{
        .status = ResultStatus::kInvalidLeader,
    };
  }

  std::unique_lock<std::mutex> lk{mtx_};

  auto promise = fn::make_shared<std::promise<Result<ZkResponse>>>();
  auto future  = promise->get_future();

  auto result_cb = [this, s = self(), promise](Result<Data> result) {
    if (!result) {
      shutdown();
      return promise->set_value(result.error());
    }

    if (auto r = from_data<ZkResponse>(std::move(result.value())); !r) {
      shutdown(r.error());
      promise->set_value(r.error());
    } else {
      set_leader(r.value().status != ResultStatus::kInvalidLeader);
      promise->set_value(r);
    }
  };
  write_request(request, result_cb);

  if (future.wait_for(std::chrono::seconds(2)) == std::future_status::ready)
    return future.get();

  LOG_ERROR << "timeout waiting for result";

  boost::system::error_code ec;
  ec.assign(ETIMEDOUT, boost::system::system_category());
  shutdown(ec);

  return ec;
}

void RaftServerConnection::write_request(ZkRequest const&                  request,
                                         std::function<void(Result<Data>)> result_cb) {
  write_data(to_data(request),
             [this, s = self(), result_cb = std::move(result_cb)](Result<void> r) {
               if (!r) {
                 shutdown(r.error());
                 return result_cb(r.error());
               } else
                 read_data(std::move(result_cb));
             });
}

