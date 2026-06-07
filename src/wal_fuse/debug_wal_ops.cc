
#include "src/base/logger.h"
#include "src/wal_fuse/wal_fuse_ops.h"

struct WriteRequest {
  char   path[32];
  size_t size;
  off_t  off;
};

bool fread(int fd, void* buf, size_t count) {
  size_t size = 0;
  while (size < count) {
    ssize_t rc = ::read(fd, reinterpret_cast<char*>(buf) + size, count - size);
    if (rc < 0)
      log(FATAL, "read failed {} ({})", strerror(errno), errno);
    if (rc == 0 && size)
      log(FATAL, "read failed {} ({}) after reading {}/{}", strerror(errno), errno, size, count);

    if (rc == 0)
      return false;

    size += static_cast<size_t>(rc);
  }
  return true;
}

std::filesystem::path tmp{"tmp"};

int wal_file(char const* path) {
  std::string s = tmp.c_str();
  s += path;

  char ch = '\0';

  int fd_1 = ::open(s.c_str(), O_WRONLY | O_CREAT | O_EXCL, file_mode());
  s += ".ref";

  if (fd_1 == -1) {
    return ::open(s.c_str(), O_RDWR, file_mode());
  }

  int fd_2 = ::open(s.c_str(), O_RDWR | O_CREAT | O_EXCL, file_mode());
  if (fd_2 == -1)
    log(FATAL, "unable to open {}", s);

  if (strcmp(path, "/000000010000000000000001") == 0) {
    int fds = ::open("/tmp/000000010000000000000001", O_RDONLY, file_mode());
    if (fds == -1)
      log(FATAL, "unable to read 000000010000000000000001, {} ({})", strerror(errno), errno);

    for (unsigned i = 0; i < kWalSegmentSize; ++i) {
      if (::read(fds, &ch, 1) != 1)
        log(FATAL, "unable to read wal file");
      if (::write(fd_1, &ch, 1) != 1)
        log(FATAL, "unable to initialize wal file");
      if (::write(fd_2, &ch, 1) != 1)
        log(FATAL, "unable to initialize wal file");
    }
    ::close(fds);
  } else {
    for (unsigned i = 0; i < kWalSegmentSize; ++i) {
      if (::write(fd_1, &ch, 1) != 1)
        log(FATAL, "unable to initialize wal file");
      if (::write(fd_2, &ch, 1) != 1)
        log(FATAL, "unable to initialize wal file");
    }
  }
  ::close(fd_1);
  return fd_2;
}

int main() {
  logger::set_level(logger::Level::Info);

  std::filesystem::remove_all(tmp);
  std::filesystem::create_directories(tmp);

  int fd = ::open("/tmp/wal_data", O_RDONLY, file_mode());
  if (fd == -1)
    log(FATAL, "open failed {} ({})", strerror(errno), errno);

  WalTracker tracker;
  WalFuseOps ops{tracker, tmp};

  std::map<std::string, std::pair<int, struct fuse_file_info>> files;

  std::vector<char> buffer;

  WriteRequest r;
  while (fread(fd, &r, sizeof(WriteRequest))) {
    if (r.size == 0) {
      if (r.off == 1) {
        int fd = wal_file(r.path);

        files[r.path]              = std::make_pair(fd, fuse_file_info{});
        files[r.path].second.flags = O_RDWR;
        ops.op_open(r.path, &(files.find(r.path)->second.second));
      } else {
        ops.op_release(r.path, &(files.find(r.path)->second.second));
        ::close(files.find(r.path)->second.first);

        files.erase(r.path);
      }
    } else {
      buffer.resize(r.size);
      bool b = fread(fd, buffer.data(), r.size);
      if (!b)
        log(FATAL, "buffer read failed");

      auto pos = files.find(r.path);

      ::pwrite(pos->second.first, buffer.data(), r.size, r.off);
      ::fsync(pos->second.first);

      if (r.path[24] == '2' && r.off > (4427451 - (4 * 4096)))
        logger::set_level(logger::Level::Debug);

      ops.op_write(r.path, buffer.data(), r.size, r.off, &(pos->second.second));
    }
  }
  return 0;
}
