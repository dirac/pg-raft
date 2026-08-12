#include "src/raft/raft_server_connection.h"

Result<StatusResponse> RaftServerConnection::node_status() {
  std::unique_lock<std::mutex> lk{mtx_};

  write_data(to_data(ClientRequest{StatusRequest{}}), [](Result<void>) {});

  auto promise = make_shared<std::promise<Result<StatusResponse>>>();
  auto future  = promise->get_future();

  auto result_cb = [this, s = self(), promise](Result<Data> result) {
    if (!result) {
      shutdown();
      return promise->set_value(result.error());
    }

    if (auto r = from_data<StatusResponse>(std::move(result.value())); !r) {
      shutdown(r.error());
      promise->set_value(r.error());
    } else
      promise->set_value(r);
  };
  read_data(result_cb);

  if (future.wait_for(std::chrono::seconds(2)) == std::future_status::ready)
    return future.get();
  log(ERROR, "timeout waiting for result");

  boost::system::error_code ec;
  ec.assign(ETIMEDOUT, boost::system::system_category());
  shutdown(ec);

  return ec;
}

