#include "src/raft/raft_server_connection.h"

Result<Data> RaftServerConnection::read(std::chrono::seconds timeout) {
  auto promise = make_shared<std::promise<Result<Data>>>();
  auto future  = promise->get_future();

  read_data([promise](Result<Data> result) { promise->set_value(std::move(result)); });

  if (future.wait_for(timeout) == std::future_status::ready)
    return future.get();

  log(ERROR, "timeout waiting for result");
  auto ec = system_error_code(ETIMEDOUT);
  shutdown(ec);
  return ec;
}

Result<StatusResponse> RaftServerConnection::node_status() {
  std::unique_lock<std::mutex> lk{mtx_};

  write_data(to_data(ClientRequest{StatusRequest{}}));

  auto r = read(std::chrono::seconds(2));
  if (!r)
    return r.error();
  return from_data<StatusResponse>(r.value());
}

Result<void> RaftServerConnection::backup(std::filesystem::path const& dir, RaftIndex applied_idx) {
  std::unique_lock<std::mutex> lk{mtx_};

  if (std::filesystem::exists(dir))
    return elog(system_error_code(EEXIST), "The backup directory {} exists", dir);

  write_data(to_data(ClientRequest{SnapshotRequest{.applied_idx = applied_idx}}));

  // A backup can take a while (pg_basebackup, or stopping a standby and copying its data).
  auto r = read(std::chrono::seconds(3600));
  if (!r)
    return r.error();

  auto response = from_data<SnapshotResponse>(r.value());
  if (!response)
    return response.error();
  if (!response.value().success)
    return elog(system_error_code(EAGAIN), "{} refused the backup", node_id());

  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  if (ec)
    return elog(ec, "Unable to create {}", dir);

  for (auto const& name : response.value().files) {
    auto promise = make_shared<std::promise<Result<std::filesystem::path>>>();
    auto future  = promise->get_future();
    read_file([promise](Result<std::filesystem::path> f) { promise->set_value(std::move(f)); });

    auto file = future.get();
    if (!file)
      return elog(file.error(), "Receiving {}", name);

    auto dst = dir / name;
    std::filesystem::create_directories(dst.parent_path(), ec);
    if (!ec)
      std::filesystem::rename(file.value(), dst, ec);
    if (ec)
      return elog(ec, "Moving {} to {}", file.value(), dst);
  }

  for (auto const& [name, target] : response.value().links) {
    auto link = dir / name;
    std::filesystem::create_directories(link.parent_path(), ec);
    if (!ec)
      std::filesystem::create_symlink(target, link, ec);
    if (ec)
      return elog(ec, "Creating the link {} to {}", link, target);
  }
  return ok();
}
