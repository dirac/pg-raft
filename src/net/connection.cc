#include "src/net/connection.h"

#include "src/base/logger.h"
#include "src/base/xxhash.h"

namespace {
std::atomic_bool g_frame_checksums{false};
}  // namespace

void Connection::set_frame_checksums(bool on) {
  g_frame_checksums.store(on, std::memory_order_relaxed);
}

bool Connection::frame_checksums() {
  return g_frame_checksums.load(std::memory_order_relaxed);
}

using ::boost::system::error_code;

namespace {

Result<size_t> fwrite(int fd, void const* buf, size_t count) {
  size_t size = 0;
  while (size < count) {
    ssize_t rc = ::write(fd, reinterpret_cast<char const*>(buf) + size, count - size);
    if (rc <= 0)
      return system_error_code(errno);
    size += static_cast<size_t>(rc);
  }
  return size;
}

Result<size_t> fread(int fd, void* buf, size_t count) {
  size_t size = 0;
  while (size < count) {
    ssize_t rc = ::read(fd, reinterpret_cast<char*>(buf) + size, count - size);
    if (rc <= 0)
      return system_error_code(errno);
    size += static_cast<size_t>(rc);
  }
  return size;
}

}  // namespace

Connection::Connection(string peer, socket_type socket, std::chrono::milliseconds timeout)
    : peer_(std::move(peer)),
      io_context_(std::visit(
          [](auto& s) -> boost::asio::io_context& {
            return static_cast<boost::asio::io_context&>(s.get_executor().context());
          },
          socket)),
      socket_(std::move(socket)),
      strand_(io_context_),
      timeout_timer_(io_context_),
      timeout_(std::get<0>(timeout_and_times(timeout))),
      times_(std::get<1>(timeout_and_times(timeout))),
      delay_timer_(io_context_),
      log_prefix_(peer_.c_str()) {
  if (!peer_.empty())
    set_log_prefix(format("({0}) ", peer_));

  assert(!closed());
}

Connection::~Connection() {
  if (test_and_set(closed_))
    return;

  log(INFO, "{}Connection terminated without calling shutdown!!!", log_prefix());
}

boost::asio::io_context& Connection::io_context() {
  switch (socket_.index()) {
    case 0:
      return static_cast<boost::asio::io_context&>(std::get<0>(socket_).get_executor().context());
    case 1:
      return static_cast<boost::asio::io_context&>(std::get<1>(socket_).get_executor().context());
  }
  std::unreachable();
}

void Connection::on_shutdown() {}

void Connection::start() {
  [[maybe_unused]] auto const started = test_and_set(started_);
  assert(!started);

  do_read_header();
  if (timeout_.count() > 0 && times_ > 0) {
    request_peer_status();
    enqueue_timeout_check();
  }
}

void Connection::enqueue_timeout_check(unsigned times, uint64_t last_counter) {
  if (closed())
    return;

  // If the counter is the same for the last times_ checks (i.e. timeout) then we have not
  // received anything.

  auto curr = counter();
  if (curr == last_counter && times > times_) {
    return boost::asio::post(io_context(), [this, s = self()] {
      error_code ec;
      ec.assign(ETIMEDOUT, boost::system::system_category());
      shutdown(ec);
    });
  }
  ++times;
  if (last_counter != curr) {
    times        = 0;
    last_counter = curr;
  }

  request_peer_status();
  timeout_timer_.async_wait(timeout_, [this, s = self(), times, last_counter](error_code) {
    enqueue_timeout_check(times, last_counter);
  });
}

uint8_t Connection::status() const {
  return 0;
}

void Connection::set_peer_status(uint8_t) {}

template <typename M>
void Connection::async_read(std::function<void(Result<M>)> handler, ReadQ<M>& q) {
  std::unique_lock<std::mutex> lk{q.mtx};
  if (closed() && q.queue.empty()) {
    lk.unlock();

    boost::asio::post(io_context(), [handler = std::move(handler)]() mutable {
      error_code ec = boost::asio::error::operation_aborted;
      handler(ec);
    });
  } else if (!q.queue.empty()) {
    auto elem = std::move(q.queue.front().v);
    q.queue.pop_front();
    lk.unlock();

    ::boost::asio::post(io_context(),
                        [elem = std::move(elem), handler = std::move(handler)]() mutable {
                          handler(std::move(elem));
                        });
  } else {
    q.handlers.emplace_back(std::move(handler));
  }
}

template <typename M>
void Connection::enqueue(Result<M> elem, ReadQ<M>& q) {
  std::unique_lock<std::mutex> lk{q.mtx};
  if (!q.handlers.empty()) {
    auto handler = std::move(q.handlers.front().f);
    q.handlers.pop_front();
    lk.unlock();

    // On the thread that read it: posting it cost a wake-up of another thread per message (on
    // raft's commit path, one on each side).
    handler(std::move(elem));
  } else {
    q.queue.emplace_back(std::move(elem));
  }
}

template <typename M>
void Connection::shutdown(error_code ec, ReadQ<M>& q) {
  std::unique_lock<std::mutex> lk{q.mtx};
  while (!q.handlers.empty()) {
    auto handler = std::move(q.handlers.front().f);
    q.handlers.pop_front();

    boost::asio::post(io_context(),
                      [ec, handler = std::move(handler)]() mutable { handler(std::move(ec)); });
  }
}

void Connection::read_data_and_buff(std::function<void(Result<std::tuple<Data, Bytes>>)> handler) {
  assert(handler);
  async_read<std::tuple<Data, Bytes>>(std::move(handler), read_data_q_);
}

void Connection::read_file(std::function<void(Result<std::filesystem::path>)> handler) {
  assert(handler);
  async_read<std::filesystem::path>(std::move(handler), read_file_q_);
}

void Connection::process_header() {
  // On an invalid header stop reading: its sizes cannot be trusted, and breakpoint() has
  // posted the shutdown of the connection.
  if (read_header_[0] != hton(0xDEADBEEF)) [[unlikely]]
    return breakpoint("Invalid start of the packet");

  if (++recv_packet_ != ntoh(read_header_[1])) [[unlikely]]
    return breakpoint("Invalid counter");

  uint32_t const h = ntoh(read_header_[2]);

  // Significant bits
  //  b11 status req/resp
  //  b0  message
  //  b10 file (the next word has the low 32 bits of the size).

  auto ctrl = h >> 30U;
  if (ctrl == 0x3) {
    if ((h & 0xFFU) != 0) {
      clear(outstanding_status_request_);
      set_peer_status(u1(h & 0xFFU) - u1(1U));
    } else {
      // this is a status request, so enqueue a status response to the write q.
      async_write_status(false);
    }
    return do_read_header();
  }

  if (ctrl == 0x2) {
    uint32_t h1 = ntoh(read_header_[2]);
    uint32_t h2 = ntoh(read_header_[3]);

    // Remove the High bit.
    h1 = (h1 << 1U) >> 1U;

    uint64_t length = static_cast<uint64_t>(h1) << 32U | h2;
    return do_read_file(length);
  }

  uint32_t data_size = ntoh(read_header_[2]);
  uint32_t buff_size = ntoh(read_header_[3]);
  uint32_t data_chk  = ntoh(read_header_[4]);
  uint32_t buff_chk  = ntoh(read_header_[5]);

  do_read_data(data_size, buff_size, data_chk, buff_chk);
}

void Connection::do_read_header() {
  if (closed())
    return;

  auto handler = [this, s = self()](error_code const& ec, size_t) {
    clear(outstanding_read_req_);

    if (ec)
      return shutdown(ec);

    process_header();
  };
  async_read(boost::asio::mutable_buffer(read_header_.data(), sizeof(read_header_)),
             std::move(handler));
}

void Connection::breakpoint(char const* data) {
  log(ERROR, "{}{}", log_prefix(), data);

  // Not called directly: the write mutex may be held by the caller, shutdown() takes it.
  boost::asio::post(io_context(), [this, s = self()] { shutdown(); });
}

void Connection::do_read_data(uint32_t data_size,
                              uint32_t buff_size,
                              uint32_t data_chk,
                              uint32_t buff_chk) {
  Data  data{data_size};
  Bytes buff(buff_size);

  assert(buff.size() == buff_size);
  assert(data.capacity() == data_size);

  auto buffs = data.mutable_buffer();
  if (buff_size != 0U)
    buffs.emplace_back(buff.data(), buff.size());

  auto handler =
      [this, s = self(), data = std::move(data), buff = std::move(buff), data_chk, buff_chk](
          error_code const& ec, size_t length) mutable {
        clear(outstanding_read_req_);
        if (ec) {
          enqueue<ReadData>(ec, read_data_q_);
          return shutdown(ec);
        }
        assert(length == (data.capacity() + buff.size()));
        (void)length;

        // A checksum of 0: the sender didn't compute it (frame_checksums() off there).
        if (auto v = data_chk ? xxhash32(data.const_buffer()) : 0U; v != data_chk) {
          auto ec = system_error_code(EBADMSG);
          log(ERROR, "{}Data checksum did not match {} vs {}", log_prefix(), data_chk, v);
          enqueue<ReadData>(ec, read_data_q_);
          return shutdown(ec);
        }

        if (auto v = buff_chk ? xxhash32(buff) : 0U; v != buff_chk) {
          auto ec = system_error_code(EBADMSG);
          log(ERROR, "{}Buffer checksum did not match {} vs {}", log_prefix(), buff_chk, v);
          enqueue<ReadData>(ec, read_data_q_);
          return shutdown(ec);
        }

        enqueue<ReadData>(std::make_tuple(std::move(data), std::move(buff)), read_data_q_);
        return do_read_header();
      };

  async_read(std::move(buffs), std::move(handler));
}

void Connection::cleanup(std::error_code ec, int fd, std::filesystem::path const& file) {
  if (fd != -1)
    ::close(fd);
  enqueue<std::filesystem::path>(ec, read_file_q_);

  // Remove the file and ignore the error.
  std::filesystem::remove(file, ec);
}

void Connection::do_read_file(uint64_t length) {
  if (!recv_file_buff_)
    recv_file_buff_ = make_unique<std::array<char, kFileTransferSize + 4U>>();

  auto file = tmp_file();

  int fd = ::open(file.c_str(), O_RDWR | O_CREAT | O_EXCL, file_mode());
  if (fd == -1) {
    auto ec = system_error_code(errno);

    cleanup(ec, fd, file);
    return shutdown(ec);
  }
  do_read_file(self(), fd, length, std::move(file));
}

void Connection::do_read_file(shared_ptr<Connection> self,
                              int                    fd,
                              uint64_t               length,
                              std::filesystem::path  file) {
  if (length == 0) {
    ::close(fd);
    enqueue<std::filesystem::path>(std::move(file), read_file_q_);
    return do_read_header();
  }

  auto handler = [this, s = std::move(self), fd, length, file](error_code const& ec,
                                                               size_t            bytes) mutable {
    clear(outstanding_read_req_);

    if (ec) {
      cleanup(ec, fd, file);
      return shutdown(ec);
    }

    static_assert(sizeof(uint32_t) == 4);

    uint32_t chk = 0;
    std::memcpy(&chk, recv_file_buff_->data(), sizeof(chk));
    chk = ntoh(chk);

    if (auto v = xxhash32(boost::asio::const_buffer(recv_file_buff_->data() + 4U, bytes - 4U));
        v != chk) {
      auto ec = system_error_code(EBADMSG);
      log(ERROR, "{}Checksum did not match {} vs {}", log_prefix(), chk, v);
      cleanup(ec, fd, file);
      return shutdown(ec);
    }

    if (auto r = fwrite(fd, recv_file_buff_->data() + 4U, bytes - 4U); !r) {
      cleanup(r.error(), fd, file);
      return shutdown(r.error());
    }
    return do_read_file(std::move(s), fd, length - (bytes - 4U), std::move(file));
  };

  async_read(boost::asio::mutable_buffer(recv_file_buff_->data(),
                                         std::min<size_t>(length + 4U, recv_file_buff_->size())),
             std::move(handler));
}

template <typename M>
void Connection::async_write(M                                 elem,
                             std::function<void(Result<void>)> handler,
                             WriteQ<M>&                        q,
                             WriteToken                        token) {
  assert(handler);
  std::unique_lock<std::mutex> lk{write_mtx_};

  if (closed()) {
    boost::asio::post(io_context(), [handler = std::move(handler)]() mutable {
      ::boost::system::error_code ec = boost::asio::error::operation_aborted;
      handler(ec);
    });

  } else {
    q.queue.emplace_back(std::move(elem));
    q.handlers.emplace_back(std::move(handler));

    write_queue_.emplace_back(token);

    // Nothing in flight: start the write here (the socket's calls are serialized by its mutex, and
    // only one write is in flight), rather than waking an io thread to do it.
    if (write_queue_.size() == 1)
      do_write(std::move(lk));
  }
}

void Connection::async_write_status(bool request) {
  std::unique_lock<std::mutex> lk{write_mtx_};

  if (closed())
    return;

  // If we have an outstanding status request don't send one more.
  if (request && test_and_set(outstanding_status_request_))
    return;

  auto ch = request ? WriteToken::kStatusRequest : WriteToken::kStatusResponse;

  if (write_queue_.empty()) {
    write_queue_.emplace_back(ch);
    boost::asio::post(io_context(), boost::asio::bind_executor(strand_, [this, s = self()] {
                        do_write(std::unique_lock<std::mutex>{write_mtx_});
                      }));
  } else {
    write_queue_.insert(write_queue_.begin() + 1, ch);
  }
}

void Connection::write_test_delay() {
  std::unique_lock<std::mutex> lk{write_mtx_};

  if (closed())
    return;

  auto ch = WriteToken::kInsertTestDelay;

  if (write_queue_.empty()) {
    write_queue_.emplace_back(ch);
    boost::asio::post(io_context(), boost::asio::bind_executor(strand_, [this, s = self()] {
                        do_write(std::unique_lock<std::mutex>{write_mtx_});
                      }));
  } else {
    write_queue_.insert(write_queue_.begin() + 1, ch);
  }
}

template <typename M>
void Connection::dequeue(WriteQ<M>& q, Result<void> r) {
  // (The caller has checked that the connection is not closed.)
  auto h = std::move(q.handlers.front());
  boost::asio::post(io_context(),
                    [h = std::move(h), r = std::move(r)]() mutable { h(std::move(r)); });

  q.handlers.pop_front();
  q.queue.pop_front();
}

template <typename M>
void Connection::shutdown(error_code ec, WriteQ<M>& q) {
  std::unique_lock<std::mutex> lk{write_mtx_};

  while (!q.handlers.empty()) {
    auto h = std::move(q.handlers.front());
    boost::asio::post(io_context(), [h = std::move(h), ec]() mutable { h(ec); });

    q.handlers.pop_front();
    q.queue.pop_front();
  }
}

void Connection::dequeue(Result<void> r) {
  dequeue(std::move(r), std::unique_lock<std::mutex>{write_mtx_});
}

void Connection::dequeue(Result<void> r, std::unique_lock<std::mutex> lk) {
  if (closed())
    return;

  bool const has_error = r.has_error();
  switch (write_queue_.front()) {
    case WriteToken::kData:
      dequeue(write_data_q_, std::move(r));
      break;

    case WriteToken::kFile:
      dequeue(write_file_q_, std::move(r));
      break;

    case WriteToken::kStatusRequest:
    case WriteToken::kStatusResponse:
    case WriteToken::kInsertTestDelay:
      break;
  }
  write_queue_.pop_front();

  if (!has_error && !write_queue_.empty())
    do_write(std::move(lk));
}

void Connection::write_data_internal(Data                              data,
                                     ExtraBuffers                      buff,
                                     std::function<void(Result<void>)> handler) {
  if (closed()) {
    boost::asio::post(io_context(), [handler = std::move(handler)]() mutable {
      error_code ec = boost::asio::error::connection_aborted;
      handler(ec);
    });
    return;
  }

  // The connection can be shut down before the message is queued.

  /// Add it to the queue.
  async_write(std::make_tuple(std::move(data), std::move(buff)),
              std::move(handler),
              write_data_q_,
              WriteToken::kData);
}

void Connection::write_file(std::filesystem::path file, std::function<void(Result<void>)> handler) {
  assert(handler);

  if (closed()) {
    boost::asio::post(io_context(), [handler = std::move(handler)]() mutable {
      error_code ec = boost::asio::error::connection_aborted;
      handler(ec);
    });
    return;
  }

  // Add the request to the queue.
  async_write(std::move(file), std::move(handler), write_file_q_, WriteToken::kFile);
}

void Connection::do_write(std::unique_lock<std::mutex> lk) {
  if (closed())
    return;

  if (write_queue_.front() == WriteToken::kInsertTestDelay) {
    return delay_timer_.async_wait(std::chrono::seconds(5),
                                   [this, s = self()](error_code) { dequeue(ok()); });
  }

  // The connection can be shut down (its flag is set) before the write starts.

  write_header_[0] = hton(0xDEADBEEF);
  write_header_[1] = hton(++sent_packet_);
  write_header_[2] = 0U;
  write_header_[3] = 0U;
  write_header_[4] = 0U;
  write_header_[5] = 0U;

  switch (write_queue_.front()) {
    case WriteToken::kStatusResponse: {
      // Status response.
      // The high two bits are b11
      uint8_t  s = status();
      uint32_t h = (static_cast<uint32_t>(0x3U) << 30U) | (static_cast<uint32_t>(s) + 1U);

      write_header_[2] = hton(h);

      auto handler = [this, s = self()](error_code const& ec, size_t) {
        clear(outstanding_write_req_);

        if (ec) {
          dequeue(ec);
          return shutdown(ec);
        }
        dequeue(ok());
      };
      async_write(boost::asio::const_buffer(write_header_.data(), sizeof(write_header_)),
                  std::move(handler));
    } break;

    case WriteToken::kStatusRequest: {
      // Status request.
      // The high two bits are b11
      uint32_t h       = static_cast<uint32_t>(0x3U) << 30U;
      write_header_[2] = hton(h);

      auto handler = [this, s = self()](error_code const& ec, size_t) {
        clear(outstanding_write_req_);

        if (ec) {
          dequeue(ec);
          return shutdown(ec);
        }
        dequeue(ok());
      };
      async_write(boost::asio::const_buffer(write_header_.data(), sizeof(write_header_)),
                  std::move(handler));
    } break;

    case WriteToken::kData: {
      auto [data, buff] = std::move(write_data_q_.queue.front());

      std::vector<::boost::asio::const_buffer> extra;
      for (auto const& b : buff)
        if (b.size() != 0)
          extra.emplace_back(b);

      auto buff_size = std::accumulate(
          std::begin(extra), std::end(extra), size_t{0}, [](size_t result, auto const& b) {
            return result + b.size();
          });

      // The high bit is b0
      uint32_t h = u4(data.capacity());
      assert(h < (0x1ULL << 31U));

      write_header_[2] = hton(h);
      write_header_[3] = hton(u4(buff_size));
      if (frame_checksums()) {
        write_header_[4] = hton(xxhash32(data.const_buffer()));
        write_header_[5] = hton(xxhash32(extra));
      }

      std::vector<::boost::asio::const_buffer> buffers = data.const_buffer();
      buffers.emplace(buffers.begin(),
                      ::boost::asio::const_buffer(write_header_.data(), sizeof(write_header_)));
      buffers.insert(buffers.end(), extra.begin(), extra.end());

      auto handler = [this, s = self(), data = std::move(data)](error_code const& ec,
                                                                size_t) mutable {
        clear(outstanding_write_req_);
        if (ec) {
          dequeue(ec);
          return shutdown(ec);
        }
        dequeue(ok());
      };
      async_write(buffers, std::move(handler));

    } break;

    case WriteToken::kFile: {
      if (!send_file_buff_)
        send_file_buff_ = make_unique<std::array<char, kFileTransferSize + 4U>>();

      auto ec   = std::error_code{};
      auto file = std::move(write_file_q_.queue.front());

      auto fd = ::open(file.c_str(), O_RDONLY);
      if (fd == -1) {
        ec = system_error_code(errno);

        dequeue(ec, std::move(lk));
        return shutdown(ec);
      }

      auto size = std::filesystem::file_size(file, ec);
      if (ec) {
        ::close(fd);
        dequeue(ec, std::move(lk));
        return shutdown(ec);
      }
      assert(size < (static_cast<uint64_t>(0x1ULL) << 60U));

      // The high two bits are b10
      write_header_[2] = hton(u4(size >> 32U) | u4(0x1ULL << 31U));
      write_header_[3] = hton(u4(size & 0xFFFFFFFF));

      auto handler = [this, s = self(), fd, size](error_code const& ec, size_t) mutable {
        clear(outstanding_write_req_);
        if (ec) {
          ::close(fd);
          dequeue(ec);
          return shutdown(ec);
        }
        do_write_file(std::move(s), fd, size);
      };
      async_write(boost::asio::const_buffer(write_header_.data(), sizeof(write_header_)),
                  std::move(handler));
    } break;

    case WriteToken::kInsertTestDelay:
      std::abort();
  }
}

void Connection::do_write_file(shared_ptr<Connection> self, int fd, size_t size) {
  if (size == 0) {
    ::close(fd);
    return dequeue(ok());
  }

  auto bytes = std::min(size, send_file_buff_->size() - 4U);

  if (auto r = fread(fd, send_file_buff_->data() + 4U, bytes); !r) {
    ::close(fd);
    dequeue(r.error());
    return shutdown(r.error());
  }
  uint32_t chk = xxhash32(boost::asio::const_buffer(send_file_buff_->data() + 4U, bytes));

  chk = hton(chk);
  std::memcpy(send_file_buff_->data(), &chk, sizeof(uint32_t));

  auto buff = boost::asio::const_buffer(send_file_buff_->data(), bytes + 4U);

  auto handler = [this, s = std::move(self), fd, size](error_code const& ec,
                                                       size_t            length) mutable {
    clear(outstanding_write_req_);

    if (ec) {
      ::close(fd);
      dequeue(ec);
      return shutdown(ec);
    }

    size -= (length - 4U);

    do_write_file(std::move(s), fd, size);
  };
  async_write(buff, std::move(handler));
}

void Connection::shutdown(boost::system::error_code const& ec, std::source_location loc) {
  if (test_and_set(closed_))
    return;

  timeout_timer_.cancel();

  shutdown(boost::asio::error::connection_aborted, write_data_q_);
  shutdown(boost::asio::error::connection_aborted, write_file_q_);
  shutdown(boost::asio::error::connection_aborted, read_data_q_);
  shutdown(boost::asio::error::connection_aborted, read_file_q_);

  log(INFO,
      "{}Close connection to {} ({}:{}) error = {}",
      log_prefix(),
      peer(),
      loc.file_name(),
      loc.line(),
      ec.message());
  shutdown(self());
}

void Connection::shutdown(std::source_location loc) {
  if (test_and_set(closed_))
    return;

  timeout_timer_.cancel();

  shutdown(boost::asio::error::connection_aborted, write_data_q_);
  shutdown(boost::asio::error::connection_aborted, write_file_q_);
  shutdown(boost::asio::error::connection_aborted, read_data_q_);
  shutdown(boost::asio::error::connection_aborted, read_file_q_);
  log(INFO, "{}Close connection to {} ({}:{})", log_prefix(), peer(), loc.file_name(), loc.line());

  shutdown(self());
}

void Connection::do_shutdown(ssl_socket& stream) {
  boost::system::error_code cancel_ec;
  stream.lowest_layer().cancel(cancel_ec);

  timeout_timer_.async_wait(
      std::chrono::seconds(5),
      boost::asio::bind_executor(strand_,
                                 [s = self(), &stream](boost::system::error_code const& ec) {
                                   if (!ec) {
                                     boost::system::error_code ignored_ec;
                                     stream.lowest_layer().close(ignored_ec);
                                   }
                                 }));

  stream.async_shutdown(boost::asio::bind_executor(
      strand_, [this, s = self(), &stream](boost::system::error_code const&) {
        boost::system::error_code ignored_ec;
        if (timeout_timer_.cancel()) {
          stream.lowest_layer().shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored_ec);
          stream.lowest_layer().close(ignored_ec);
        }
      }));
}

void Connection::do_shutdown(tcp_socket& s) {
  boost::system::error_code ignored_ec;
  s.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored_ec);
  s.close(ignored_ec);
}

void Connection::shutdown(shared_ptr<Connection> self) {
  boost::asio::post(strand_, [this, self = std::move(self)] {
    std::visit([this](auto&& s) { this->do_shutdown(s); }, socket_);
  });
  on_shutdown();
}
