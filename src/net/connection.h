#pragma once

#include "src/net/data.h"
#include "src/net/host_address.h"
#include "src/net/sha256.h"

using tcp_socket  = boost::asio::ip::tcp::socket;
using ssl_socket  = boost::asio::ssl::stream<boost::asio::ip::tcp::socket>;
using socket_type = std::variant<tcp_socket, ssl_socket>;

class Connection : public std::enable_shared_from_this<Connection> {
 public:
  Connection(string                    peer,
             socket_type               socket,
             std::chrono::milliseconds timeout = std::chrono::milliseconds(0));

  virtual ~Connection();

  template <class ConnectionType, typename... Args>
  static shared_ptr<ConnectionType> create(Args&&... args) {
    auto s = std::make_shared<ConnectionType>(std::forward<Args>(args)...);
    s->start();
    return s;
  }

  template <typename ConnectionType = Connection>
  shared_ptr<ConnectionType> self() {
    if constexpr (std::is_same_v<ConnectionType, Connection>)
      return std::enable_shared_from_this<Connection>::shared_from_this();
    else
      return std::dynamic_pointer_cast<ConnectionType>(
          std::enable_shared_from_this<Connection>::shared_from_this());
  }

  string const& peer() const { return peer_; }

  virtual void start();

  // Status value range is [254, 0]
  virtual uint8_t status() const;
  virtual void    set_peer_status(uint8_t s);

  void read_data(std::function<void(Result<Data>)> handler);
  void read_file(std::function<void(Result<std::filesystem::path>)> handler);

  void write_data(Data                                         data,
                  inplace_vector<boost::asio::const_buffer, 2> buff,
                  std::function<void(Result<void>)>            handler);
  void write_data(Data data, std::function<void(Result<void>)> handler) {
    write_data(std::move(data), {}, std::move(handler));
  }
  void write_file(std::filesystem::path file, std::function<void(Result<void>)> handler);

  void request_peer_status() { async_write_status(true); }

  void shutdown(boost::system::error_code const& ec,
                std::source_location             loc = std::source_location::current());
  void shutdown(std::source_location loc = std::source_location::current());

  bool closed() const { return closed_.test(std::memory_order_relaxed); }

  /// Function called when the connect is getting shutdown.
  virtual void on_shutdown();

  bool ssl_enabled() const { return socket_.index() == 1; }

  void set_log_prefix(string prefix) {
    std::unique_lock<std::mutex> lk{log_prefix_mtx_};
    log_prefixes_.emplace_back(std::move(prefix));
    log_prefix_.store(log_prefixes_.back().c_str());
  }
  char const* log_prefix() const { return log_prefix_.load(std::memory_order_relaxed); }

 private:
  /// File read and write.
  Result<size_t> fwrite(int fd, void const* buf, size_t count);
  Result<size_t> fread(int fd, void* buf, size_t count);

  void enqueue_status_req();
  void enqueue_timeout_check(unsigned curr = 0, unsigned last = 0);

  template <typename M>
  struct ReadQ {
    std::mutex                                 mtx;
    std::deque<Result<M>>                      queue;
    std::deque<std::function<void(Result<M>)>> handlers;
  };

  ReadQ<Data>                  read_data_q_;
  ReadQ<std::filesystem::path> read_file_q_;

  template <typename M>
  void async_read(std::function<void(Result<M>)> handler, ReadQ<M>& q);

  template <typename M>
  void enqueue(Result<M> elem, ReadQ<M>& q);

  template <typename M>
  void shutdown(boost::system::error_code ec, ReadQ<M>& q);

  template <typename M>
  struct WriteQ {
    std::deque<M>                                 queue;
    std::deque<std::function<void(Result<void>)>> handlers;
  };

  enum class WriteToken : char {
    kStatusRequest,
    kStatusResponse,
    kData,
    kFile,
  };

  /// Data and the subsequent buffer.
  using WriteData = std::tuple<Data, inplace_vector<boost::asio::const_buffer, 2>>;

  std::mutex                    write_mtx_;
  std::deque<WriteToken>        write_queue_;
  WriteQ<WriteData>             write_data_q_;
  WriteQ<std::filesystem::path> write_file_q_;

  template <typename M>
  void async_write(M                                 elem,
                   std::function<void(Result<void>)> handler,
                   WriteQ<M>&                        q,
                   WriteToken                        token);

  void async_write_status(bool request = true);

  template <typename M>
  void dequeue(WriteQ<M>& q, Result<void> r);

  template <typename M>
  void shutdown(boost::system::error_code ec, WriteQ<M>& q);

  void dequeue(Result<void> r);
  void dequeue(Result<void> r, std::unique_lock<std::mutex> lk);

  void do_write(std::unique_lock<std::mutex> lk);

  template <typename F>
  void do_write_header(boost::asio::const_buffer buff, F h);

  void do_write_data(shared_ptr<Connection>                       self,
                     Data                                         data,
                     inplace_vector<boost::asio::const_buffer, 2> buff);
  void do_write_file(shared_ptr<Connection>    self,
                     int                       fd,
                     size_t                    size,
                     boost::asio::const_buffer buff);
  void do_write_checksum(shared_ptr<Connection> self);
  void do_write_checksum(shared_ptr<Connection> self, asio::const_buffer buff);

  uint32_t recv_packet_ = 0;
  uint32_t sent_packet_ = 0;

  std::array<uint32_t, 4U> read_header_;
  std::array<uint32_t, 4U> write_header_;

  void process_header(unsigned idx);
  void do_read_header(unsigned idx = 0, unsigned e = 3);
  void do_read_header_some(boost::asio::mutable_buffer buff, unsigned idx);

  void cleanup(std::error_code ec, int fd, std::filesystem::path file);

  void do_read_data(shared_ptr<Connection> self, Data data);
  void do_read_data(uint32_t length);
  void do_read_file(uint64_t length);
  void do_read_file(shared_ptr<Connection> self,
                    int                    fd,
                    uint64_t               length,
                    std::filesystem::path  file);
  void do_read_checksum(shared_ptr<Connection> self, std::filesystem::path file);
  void do_read_checksum(shared_ptr<Connection>      self,
                        std::filesystem::path       file,
                        boost::asio::mutable_buffer buff);

  void shutdown(shared_ptr<Connection> self);
  void async_shutdown(std::function<void()> invoke_on_shutdown);

  template <typename T>
  void socket_close(T& socket, std::function<void()> invoke_on_shutdown);

  template <typename T>
  void socket_shutdown(T& socket, std::function<void()> invoke_on_shutdown);

  boost::asio::io_context& io_context() {
    switch (socket_.index()) {
      case 0:
        return static_cast<boost::asio::io_context&>(std::get<0>(socket_).get_executor().context());
      case 1:
        return static_cast<boost::asio::io_context&>(std::get<1>(socket_).get_executor().context());
    }
    std::unreachable();
  }

  static uint32_t ntoh(uint32_t t) { return t; }
  static uint32_t hton(uint32_t t) { return t; }

  static constexpr bool split = true;

  template <typename Handler>
  void async_read_some(boost::asio::mutable_buffer buff, Handler handler) {
    if (closed())
      return handler(boost::asio::error::connection_aborted, 0);

    switch (socket_.index()) {
      case 0:
        std::get<0>(socket_).async_read_some(
            buff, boost::asio::bind_executor(strand_, std::move(handler)));
        break;

      case 1:
        std::get<1>(socket_).async_read_some(
            buff, boost::asio::bind_executor(strand_, std::move(handler)));
        break;
    }
  }

  template <typename Handler>
  void async_read_some(boost::container::static_vector<::boost::asio::mutable_buffer, 2> buffs,
                       Handler                                                           handler) {
    assert(!buffs.empty());
    if (closed())
      return handler(boost::asio::error::connection_aborted, 0);

    switch (socket_.index()) {
      case 0:
        std::get<0>(socket_).async_read_some(
            buffs, boost::asio::bind_executor(strand_, std::move(handler)));
        break;

      case 1:
        std::get<1>(socket_).async_read_some(
            buffs, boost::asio::bind_executor(strand_, std::move(handler)));
        break;
    }
  }

  template <typename Handler>
  void async_write_some(boost::asio::const_buffer buff, Handler handler) {
    if (closed())
      return handler(boost::asio::error::connection_aborted, 0);

    switch (socket_.index()) {
      case 0:
        std::get<0>(socket_).async_write_some(
            buff, boost::asio::bind_executor(strand_, std::move(handler)));
        break;

      case 1:
        std::get<1>(socket_).async_write_some(
            buff, boost::asio::bind_executor(strand_, std::move(handler)));
        break;
    }
  }

  template <typename Handler>
  void async_write_some(boost::container::static_vector<::boost::asio::const_buffer, 2> buffs,
                        Handler                                                         handler) {
    if (closed())
      return handler(boost::asio::error::connection_aborted, 0);

    switch (socket_.index()) {
      case 0:
        std::get<0>(socket_).async_write_some(
            buffs, boost::asio::bind_executor(strand_, std::move(handler)));
        break;

      case 1:
        assert(ssl_enabled());
        std::get<1>(socket_).async_write_some(
            buffs, boost::asio::bind_executor(strand_, std::move(handler)));
        break;
    }
  }

  void set_last_epoch(std::atomic_flag& f) {
    last_epoch_.store(epoch_msec(), std::memory_order_relaxed);
    f.clear(std::memory_order_relaxed);
  }
  unsigned last_epoch() const {
    static_assert(std::atomic<unsigned>::is_always_lock_free);
    return last_epoch_.load(std::memory_order_relaxed);
  }

  // Identity of the remote peer.
  string const peer_;

  boost::asio::io_context& io_context_;

  socket_type                     socket_;
  boost::asio::io_context::strand strand_;

  // File transfers
  unique_ptr<std::array<char, kPageSize>> do_read_file_buff_;
  unique_ptr<std::array<char, kPageSize>> do_write_file_buff_;

  ChecksumContext write_checksum_ctx_;
  Checksum        write_checksum_;

  ChecksumContext read_checksum_ctx_;
  Checksum        read_checksum_;

  // The last epoch that we read a character.
  std::atomic<unsigned> last_epoch_;

  std::chrono::milliseconds const timeout_;
  boost::asio::steady_timer       timeout_timer_;

  std::chrono::milliseconds const status_;
  boost::asio::steady_timer       status_timer_;

  std::atomic_flag closed_                     = ATOMIC_FLAG_INIT;
  std::atomic_flag started_                    = ATOMIC_FLAG_INIT;
  std::atomic_flag outstanding_status_request_ = ATOMIC_FLAG_INIT;
  std::atomic_flag outstanding_read_req_       = ATOMIC_FLAG_INIT;
  std::atomic_flag outstanding_write_req_      = ATOMIC_FLAG_INIT;

  // Prepend log statements.
  std::mutex               log_prefix_mtx_;
  list<string>             log_prefixes_;
  std::atomic<char const*> log_prefix_{""};
};
