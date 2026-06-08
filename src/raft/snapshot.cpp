#include "lib/servers/zookeeper/snapshot.hpp"

#include "lib/base/cert_functions.hpp"

namespace banyandb::servers::zookeeper {

Snapshot::~Snapshot() {
  std::error_code ec;
  std::filesystem::remove(file_, ec);
  if (ec)
    LOG_ERROR << "Unable to remove file " << ec.message();

  ::unlink(file_.c_str());
}

Result<std::tuple<std::filesystem::path, string, size_t>> Snapshot::details(RaftLog const& log) {
  std::unique_lock<std::mutex> lk{mtx_};
  if (size_)
    return {file_, checksum_, size_};

  auto file = dir_ / fn::format("snapshot-{0}.lmdb", INT(log_index_));

  std::error_code ec;
  std::filesystem::remove(file, ec);
  if (ec)
    return ERROR_MSG(ec, "Unable to remove file");

  if (auto r = log.take_db_snapshot(file); !r)
    return ERROR_MSG(r.error(), "take_db_snapshot");

  string checksum;
  if (auto r = CertFunctions::sha256(file); !r)
    return ERROR_MSG(system_error_code(EINVAL), "Failed to get sha256 for the snapshot file");
  else
    checksum = r.value();

  auto size = std::filesystem::file_size(file, ec);
  if (ec)
    return ERROR_MSG(ec, "std::filesystem::file_size");

  file_     = std::move(file);
  size_     = size;
  checksum_ = std::move(checksum);

  return {file_, checksum_, size_};
}

}  // namespace banyandb::servers::zookeeper
