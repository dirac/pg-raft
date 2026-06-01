#include "src/net/connection.h"

#include "src/base/logger.h"

using error_code = ::boost::system::error_code;

Connection::Connection(string peer, socket_type socket, std::chrono::milliseconds timeout)
    : peer_(std::move(peer)),
      io_context_(std::visit(
          [](auto& s) -> boost::asio::io_context& {
            return static_cast<boost::asio::io_context&>(s.get_executor().context());
          },
          socket)),
      socket_(std::move(socket)),
      strand_(io_context_),
      last_epoch_(epoch_msec()),
      timeout_(timeout),
      timeout_timer_(io_context_),
      status_(std::min(std::chrono::milliseconds(1000), timeout / 10)),
      status_timer_(io_context_) {
  if (!peer_.empty())
    set_log_prefix(format("({0}) ", peer_));

  assert(!closed());
}

Connection::~Connection() {
  if (closed_.test_and_set())
    return;

  LOG_INFO("{} Connection terminated without calling shutdown!!!", log_prefix());
}

void Connection::on_shutdown() {}

void Connection::start() {
  assert(started_.test_and_set() == false);

  do_read_header();
  if (timeout_.count() > 0) {
    enqueue_status_req();
    enqueue_timeout_check();
  }
}

void Connection::enqueue_status_req() {
  status_timer_.expires_after(status_);
  status_timer_.async_wait([this, s = self()](error_code const&) {
    request_peer_status();
    if (!closed())
      enqueue_status_req();
  });
}

void Connection::enqueue_timeout_check(unsigned curr, unsigned last) {
  assert(last + timeout_.count() >= curr);

  auto d = last < curr ? curr - last : 0;
  auto t =
      std::max(std::chrono::milliseconds(100), std::chrono::milliseconds(timeout_.count() - d));

  timeout_timer_.expires_after(t);
  timeout_timer_.async_wait([this, s = self()](error_code ec) {
    auto c = epoch_msec();
    auto l = last_epoch();
    if (l + timeout_.count() < c) {
      LOG_INFO(
          "{} c={} l={} diff = {} timeout = {}", log_prefix(), c, l, (c - l), timeout_.count());

      ec.assign(ETIMEDOUT, boost::system::system_category());
      shutdown(ec);
    } else if (!closed())
      enqueue_timeout_check(c, l);
  });
}

uint8_t Connection::status() const {
  return 0;
}

void Connection::set_peer_status(uint8_t) {};

template <typename M>
void Connection::async_read(std::function<void(Result<M>)> handler, ReadQ<M>& q) {
  std::unique_lock<std::mutex> lk{q.mtx};
  if (closed() && q.queue.empty()) {
    lk.unlock();

    boost::asio::post(io_context(), [handler = std::move(handler)]() {
      error_code ec = boost::asio::error::operation_aborted;
      handler(ec);
    });
  } else if (!q.queue.empty()) {
    auto elem = std::move(q.queue.front());
    q.queue.pop_front();
    lk.unlock();

    ::boost::asio::post(io_context(),
                        [elem = std::move(elem), handler = std::move(handler)]() mutable {
                          handler(std::move(elem));
                        });
  } else
    q.handlers.emplace_back(std::move(handler));
}

template <typename M>
void Connection::enqueue(Result<M> elem, ReadQ<M>& q) {
  std::unique_lock<std::mutex> lk{q.mtx};
  if (!q.handlers.empty()) {
    auto handler = std::move(q.handlers.front());
    q.handlers.pop_front();
    lk.unlock();

    boost::asio::post(io_context(),
                      [elem = std::move(elem), handler = std::move(handler)]() mutable {
                        handler(std::move(elem));
                      });
  } else
    q.queue.emplace_back(std::move(elem));
}

template <typename M>
void Connection::shutdown(error_code ec, ReadQ<M>& q) {
  std::unique_lock<std::mutex> lk{q.mtx};
  while (!q.handlers.empty()) {
    auto handler = std::move(q.handlers.front());
    q.handlers.pop_front();

    boost::asio::post(io_context(), [ec = std::move(ec), handler = std::move(handler)]() mutable {
      handler(std::move(ec));
    });
  }
}

void Connection::read_data(std::function<void(Result<Data>)> handler) {
  async_read<Data>(std::move(handler), read_data_q_);
}

void Connection::read_file(std::function<void(Result<std::filesystem::path>)> handler) {
  async_read<std::filesystem::path>(std::move(handler), read_file_q_);
}

void Connection::process_header(unsigned idx) {
  if (idx == 0) {
    // We send and recv in network format.
    if (read_header_[0] != hton(0xDEADBEEF)) [[unlikely]] {
      LOG_ERROR("Invalid start of the packet");
      return shutdown();
    }

    if (++recv_packet_ != ntoh(read_header_[1])) [[unlikely]] {
      LOG_ERROR("Invalid counter");
      return shutdown();
    }

    uint32_t const h = ntoh(read_header_[2]);

    // Significant bits
    //  b11 status req/resp
    //  b0  message
    //  b10 file (read the next int32 to get the size.).

    auto ctrl = h >> 30;
    if (ctrl == 0x3) {
      if (h & 0xFF) {
        outstanding_status_request_.clear();
        set_peer_status(u1(h & 0xFFU) - u1(1U));
      } else {
        // this is a status request, so enqeue a status response to the write q.
        async_write_status(false);
      }
      do_read_header();
    } else if (ctrl == 0x2)
      do_read_header(/*idx=*/3U, /*e=*/1U);
    else
      do_read_data(h);
  } else {
    assert(idx == 3U);

    uint32_t h1 = ntoh(read_header_[2]);
    uint32_t h2 = ntoh(read_header_[3]);

    // Remove the High bit.
    h1 = (h1 << 1) >> 1;

    uint64_t length = static_cast<uint64_t>(h1) << 32 | static_cast<uint32_t>(h2);
    do_read_file(length);
  }
}

void Connection::do_read_header(unsigned idx, unsigned e) {
  if (!closed())
    do_read_header_some(
        boost::asio::mutable_buffer(read_header_.data() + idx, e * sizeof(uint32_t)), idx);
}

void Connection::do_read_header_some(boost::asio::mutable_buffer buff, unsigned idx) {
  auto handler = [this, s = self(), idx, buff](error_code const& ec, size_t length) mutable {
    set_last_epoch(outstanding_read_req_);

    if (ec)
      return shutdown(ec);

    buff += length;
    if (buff.size())
      do_read_header_some(buff, idx);
    else
      process_header(idx);
  };
  async_read_some(buff, std::move(handler));
}

void Connection::do_read_data(uint32_t length) {
  do_read_data(self(), Data{length});
}

void Connection::do_read_data(shared_ptr<Connection> self, Data data) {
  if (data.filled(0U)) {
    enqueue<Data>(std::move(data), read_data_q_);
    return do_read_header();
  }

  auto buff    = data.mutable_buffer();
  auto handler = [this, s = std::move(self), data = std::move(data)](error_code const& ec,
                                                                     size_t length) mutable {
    set_last_epoch(outstanding_read_req_);

    if (ec) {
      enqueue<Data>(ec, read_data_q_);
      return shutdown(ec);
    }

    data.filled(length);
    do_read_data(std::move(s), std::move(data));
  };

  async_read_some(buff, std::move(handler));
}

void Connection::cleanup(std::error_code ec, int fd, std::filesystem::path file) {
  if (fd != -1)
    ::close(fd);
  enqueue<std::filesystem::path>(ec, read_file_q_);

  // Remove the file and ignore the error.
  std::filesystem::remove(file, ec);
}

static mode_t file_mode() {
  // S_IRUSR  00400 user has read permission
  // S_IWUSR  00200 user has write permission
  // S_IRGRP  00040 group has read permission
  // S_IWGRP  00020 group has write permission
  // S_IROTH  00004 others have read permission

  return S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH;
}

void Connection::do_read_file(uint64_t length) {
  if (!write_file_buff_)
    write_file_buff_ = make_unique<std::array<char, kPageSize>>();

  auto file = tmp_file();
  LOG_DEBUG("Writing file from peer at {}", file.c_str());

  int fd = ::open(file.c_str(), O_RDWR | O_CREAT | O_EXCL, file_mode());
  if (fd == -1) {
    auto ec = system_error_code(errno);

    cleanup(ec, fd, file);
    return shutdown(ec);
  } else
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
    set_last_epoch(outstanding_read_req_);

    if (ec) {
      cleanup(ec, fd, file);
      return shutdown(ec);
    }

    if (auto r = fwrite(fd, write_file_buff_->data(), bytes); !r) {
      cleanup(r.error(), fd, std::move(file));
      return shutdown(r.error());
    } else
      return do_read_file(std::move(s), fd, length - bytes, std::move(file));
  };

  async_read_some(boost::asio::mutable_buffer(write_file_buff_->data(),
                                              std::min<size_t>(length, write_file_buff_->size())),
                  std::move(handler));
}

template <typename M>
void Connection::async_write(M                                 elem,
                             std::function<void(Result<void>)> handler,
                             WriteQ<M>&                        q,
                             WriteToken                        token) {
  std::unique_lock<std::mutex> lk{write_mtx_};

  if (closed()) {
    boost::asio::post(io_context(), [handler = std::move(handler)]() {
      ::boost::system::error_code ec = boost::asio::error::operation_aborted;
      handler(ec);
    });

  } else {
    q.queue.emplace_back(std::move(elem));
    q.handlers.emplace_back(std::move(handler));

    write_queue_.emplace_back(token);

    if (write_queue_.size() == 1)
      boost::asio::post(io_context(), [this, s = self()]() {
        do_write(std::unique_lock<std::mutex>{write_mtx_});
      });
  }
}

void Connection::async_write_status(bool request) {
  std::unique_lock<std::mutex> lk{write_mtx_};

  if (closed())
    return;

  // If we have an outstanding status request don't send one more.
  if (request && outstanding_status_request_.test_and_set())
    return;

  auto ch = request ? WriteToken::kStatusRequest : WriteToken::kStatusResponse;

  if (write_queue_.empty()) {
    write_queue_.emplace_back(ch);
    boost::asio::post(io_context(),
                      [this, s = self()]() { do_write(std::unique_lock<std::mutex>{write_mtx_}); });
  } else
    write_queue_.insert(write_queue_.begin() + 1, ch);
}

template <typename M>
void Connection::dequeue(WriteQ<M>& q, Result<void> r) {
  if (closed())
    return;

  auto h = std::move(q.handlers.front());
  boost::asio::post(io_context(), [h = std::move(h), r = std::move(r)]() { h(r); });

  q.handlers.pop_front();
  q.queue.pop_front();
}

template <typename M>
void Connection::shutdown(error_code ec, WriteQ<M>& q) {
  std::unique_lock<std::mutex> lk{write_mtx_};

  while (!q.handlers.empty()) {
    auto h = std::move(q.handlers.front());
    boost::asio::post(io_context(), [h = std::move(h), ec]() { h(ec); });

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

  switch (write_queue_.front()) {
    case WriteToken::kData:
      dequeue(write_data_q_, std::move(r));
      break;

    case WriteToken::kFile:
      dequeue(write_file_q_, std::move(r));
      break;

    case WriteToken::kStatusRequest:
      break;

    case WriteToken::kStatusResponse:
      break;
  }
  write_queue_.pop_front();

  if (!r.has_error() && !write_queue_.empty())
    do_write(std::move(lk));
}

void Connection::write_data(Data                              data,
                            boost::asio::const_buffer         buff,
                            std::function<void(Result<void>)> handler) {
  if (closed()) {
    error_code ec = boost::asio::error::connection_aborted;
    handler(ec);
  }

  async_write(
      std::make_tuple(std::move(data), buff), std::move(handler), write_data_q_, WriteToken::kData);
}

void Connection::write_file(std::filesystem::path file, std::function<void(Result<void>)> handler) {
  if (closed()) {
    error_code ec = boost::asio::error::connection_aborted;
    handler(ec);
  }

  async_write(std::move(file), std::move(handler), write_file_q_, WriteToken::kFile);
}

template <typename F>
void Connection::do_write_header(boost::asio::const_buffer buff, F h) {
  auto handler = [this, buff, h = std::move(h)](error_code const& ec, size_t length) mutable {
    set_last_epoch(outstanding_write_req_);

    if (ec) {
      dequeue(ec);
      return shutdown(ec);
    }

    buff += length;
    if (buff.size())
      do_write_header(buff, std::move(h));
    else
      h();
  };
  async_write_some(buff, std::move(handler));
}

void Connection::do_write(std::unique_lock<std::mutex> lk) {
  if (closed())
    return;

  write_header_[0] = hton(0xDEADBEEF);
  write_header_[1] = hton(++sent_packet_);
  write_header_[2] = 0U;
  write_header_[3] = 0U;

  switch (write_queue_.front()) {
    case WriteToken::kStatusResponse: {
      // Status response.
      // The high two bits are b11
      uint8_t  s = status();
      uint32_t h = (static_cast<uint32_t>(0x3U) << 30) | (static_cast<uint32_t>(s) + 1U);

      write_header_[2] = hton(h);

      do_write_header(boost::asio::const_buffer(write_header_.data(), (3U) * sizeof(uint32_t)),
                      [this, s = self()]() { dequeue(ok()); });
    } break;

    case WriteToken::kStatusRequest: {
      // Status request.
      // The high two bits are b11
      uint32_t h       = static_cast<uint32_t>(0x3U) << 30;
      write_header_[2] = hton(h);

      do_write_header(boost::asio::const_buffer(write_header_.data(), (3U) * sizeof(uint32_t)),
                      [this, s = self()]() { dequeue(ok()); });
    } break;

    case WriteToken::kData: {
      auto [data, buff] = std::move(write_data_q_.queue.front());

      // The high bit is b0
      uint32_t h       = u4(data.capacity() + buff.size());
      write_header_[2] = hton(h);

      assert(h < (0x1ULL << 31));

      auto handler = [this, s = self(), data = std::move(data), buff]() mutable {
        if (data.capacity())
          do_write_data(std::move(s), std::move(data), buff);
        else
          dequeue(ok());
      };

      do_write_header(boost::asio::const_buffer(write_header_.data(), (3U) * sizeof(uint32_t)),
                      std::move(handler));
    } break;

    case WriteToken::kFile: {
      if (!read_file_buff_)
        read_file_buff_ = make_unique<std::array<char, kPageSize>>();

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
      assert(size < (static_cast<uint64_t>(0x1ULL) << 60));

      // The high two bits are b10
      write_header_[2] = hton(u4(size >> 32) | u4(0x1ULL << 31));
      write_header_[3] = hton(u4(size & 0xFFFFFFFF));

      auto handler = [this, s = self(), fd, size]() mutable {
        do_write_file(std::move(s), fd, size, boost::asio::const_buffer(nullptr, 0));
      };
      do_write_header(boost::asio::const_buffer(write_header_.data(), (4U) * sizeof(uint32_t)),
                      std::move(handler));
    } break;
  }
}

void Connection::do_write_data(shared_ptr<Connection>    self,
                               Data                      data,
                               boost::asio::const_buffer buff) {
  auto buffs = data.const_buffer();
  if (buffs.capacity() > buffs.size() && buff.size())
    buffs.emplace_back(buff);

  auto handler = [this, s = std::move(self), data = std::move(data), buff](error_code const& ec,
                                                                           size_t length) mutable {
    set_last_epoch(outstanding_write_req_);
    if (ec) {
      dequeue(ec);
      return shutdown(ec);
    }

    if (data.consumed(length)) {
      buff += length;
      if (buff.size())
        do_write_data(std::move(s), std::move(data), buff);
      else
        dequeue(ok());
    } else
      do_write_data(std::move(s), std::move(data), buff);
  };
  async_write_some(buffs, std::move(handler));
}

void Connection::do_write_file(shared_ptr<Connection>    self,
                               int                       fd,
                               size_t                    size,
                               boost::asio::const_buffer buff) {
  if (size == 0) {
    ::close(fd);
    return dequeue(ok());
  }

  if (buff.size() == 0) {
    auto bytes = std::min(size, read_file_buff_->size());

    if (auto r = fread(fd, read_file_buff_->data(), bytes); !r) {
      ::close(fd);
      dequeue(r.error());
      return shutdown(r.error());
    }
    buff = boost::asio::const_buffer(read_file_buff_->data(), bytes);
  }

  auto handler = [this, s = std::move(self), fd, size, buff](error_code const& ec,
                                                             size_t            length) mutable {
    set_last_epoch(outstanding_write_req_);

    if (ec) {
      ::close(fd);
      dequeue(ec);
      return shutdown(ec);
    }

    buff += length;
    size -= length;

    do_write_file(std::move(s), fd, size, buff);
  };
  async_write_some(buff, std::move(handler));
}

void Connection::shutdown(boost::system::error_code const& ec, std::source_location loc) {
  if (closed_.test_and_set())
    return;

  shutdown(boost::asio::error::connection_aborted, write_data_q_);
  shutdown(boost::asio::error::connection_aborted, write_file_q_);
  shutdown(boost::asio::error::connection_aborted, read_data_q_);
  shutdown(boost::asio::error::connection_aborted, read_file_q_);

  // The peer called shutdown to exchange close_notify.
  if (ec == boost::asio::error::eof)
    return on_shutdown();

  // The error asio.ssl.stream:1(stream truncated) is a common Boost.Asio SSL error (or standalone
  // Asio) indicating that the underlying TCP connection was closed before a graceful SSL/TLS
  // shutdown could be completed.
  if (ec == boost::asio::ssl::error::stream_truncated)
    return on_shutdown();

  ERROR_CODE_MSG(
      ec, "Close connection to {} ({}:{})", log_prefix(), peer(), loc.file_name(), loc.line());
  shutdown(self());
}

void Connection::shutdown(std::source_location loc) {
  if (closed_.test_and_set())
    return;

  shutdown(boost::asio::error::connection_aborted, write_data_q_);
  shutdown(boost::asio::error::connection_aborted, write_file_q_);
  shutdown(boost::asio::error::connection_aborted, read_data_q_);
  shutdown(boost::asio::error::connection_aborted, read_file_q_);

  LOG_INFO("Close connection to {} ({}:{})", peer(), loc.file_name(), loc.line());
  shutdown(self());
}

template <typename T>
void Connection::socket_close(T& socket, std::function<void()> invoke_on_shutdown) {
  if (outstanding_read_req_.test() == false && outstanding_write_req_.test() == false) {
    error_code ec;
    socket.close(ec);
    invoke_on_shutdown();
  } else {
    LOG_INFO("{} Waiting, have outstanding read/write op", log_prefix());

    auto handler = [this, &socket, invoke_on_shutdown = std::move(invoke_on_shutdown)]() {
      socket_close(socket, std::move(invoke_on_shutdown));
    };
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    boost::asio::post(io_context(), boost::asio::bind_executor(strand_, std::move(handler)));
  }
}

template <typename T>
void Connection::socket_shutdown(T& socket, std::function<void()> invoke_on_shutdown) {
  boost::system::error_code ec;
  socket.shutdown(boost::asio::socket_base::shutdown_both, ec);

  if (ec)
    return invoke_on_shutdown();

  auto handler = [this, &socket, invoke_on_shutdown = std::move(invoke_on_shutdown)]() {
    socket_close(socket, std::move(invoke_on_shutdown));
  };
  boost::asio::post(io_context(), boost::asio::bind_executor(strand_, std::move(handler)));
}

void Connection::async_shutdown(std::function<void()> invoke_on_shutdown) {
  // invoke_on_shutdown has self.
  switch (socket_.index()) {
    case 0: {
      auto handler = [this, invoke_on_shutdown = std::move(invoke_on_shutdown)]() mutable {
        socket_shutdown(std::get<0>(socket_), std::move(invoke_on_shutdown));
      };
      boost::asio::post(io_context(), boost::asio::bind_executor(strand_, std::move(handler)));
    } break;

    case 1: {
      auto handler = [this, invoke_on_shutdown = std::move(invoke_on_shutdown)](
                         boost::system::error_code ec) mutable {
        if (ec)
          invoke_on_shutdown();
        else
          socket_shutdown(std::get<0>(socket_), std::move(invoke_on_shutdown));
      };
      std::get<1>(socket_).async_shutdown(boost::asio::bind_executor(strand_, std::move(handler)));
    } break;
  }
}

void Connection::shutdown(shared_ptr<Connection> self) {
  async_shutdown([self = std::move(self)]() mutable { self->on_shutdown(); });
}

Result<size_t> Connection::fwrite(int fd, void const* buf, size_t count) {
  size_t size = 0;
  while (size < count) {
    ssize_t rc = ::write(fd, reinterpret_cast<char const*>(buf) + size, count - size);
    if (rc <= 0)
      return system_error_code(errno);
    size += static_cast<size_t>(rc);
  }
  return size;
}

Result<size_t> Connection::fread(int fd, void* buf, size_t count) {
  size_t size = 0;
  while (size < count) {
    ssize_t rc = ::read(fd, reinterpret_cast<char*>(buf) + size, count - size);
    if (rc <= 0)
      return system_error_code(errno);
    size += static_cast<size_t>(rc);
  }
  return size;
}
