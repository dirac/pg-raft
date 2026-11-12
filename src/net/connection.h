#pragma once

#include "src/net/data.h"
#include "src/net/host_address.h"
#include "src/net/socket.h"

// Ported from sparrowdb lib/net/connection.hpp. Differences: no added header words (the
// kHeaderSize template), no roles, loggers or metrics, and a message can carry up to two attached
// buffers (raft sends WAL bytes from two segments).
//
// Every frame starts with six little-endian 32-bit words:
//
//   w0  0xDEADBEEF
//   w1  frame counter (1, 2, 3, ... per direction)
//   w2  top two bits: 0x = data, 10 = file, 11 = status
//
//   data:    w2 = data size (< 2^31)   w3 = attached buffer size
//            w4 = xxhash32(data)       w5 = xxhash32(attached buffer)
//            followed by the data and then the attached buffer.
//   file:    w2 = 0x80000000 | size >> 32, w3 = size & 0xFFFFFFFF
//            followed by chunks of [xxhash32 of the chunk][up to 64 KiB of content].
//   status:  w2 low byte 0 = request, s + 1 = response with peer status s.
//
// The public functions can be called from any thread; callbacks are posted to the io_context.
class Connection : public std::enable_shared_from_this<Connection> {
 public:
  Connection(string peer, socket_type socket, std::chrono::milliseconds timeout);

  Connection(string peer, socket_type socket)
      : Connection(std::move(peer), std::move(socket), std::chrono::milliseconds(0)) {}

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

  /// Read the next message and the buffer attached to it.
  void read_data_and_buff(std::function<void(Result<std::tuple<Data, Bytes>>)> handler);

  /// Read the next message, which has no attached buffer.
  void read_data(std::function<void(Result<Data>)> handler) {
    read_data_and_buff([h = std::move(handler)](Result<std::tuple<Data, Bytes>> r) mutable {
      if (!r)
        return h(r.error());
      assert(std::get<Bytes>(r.value()).empty());

      h(std::move(std::get<Data>(r.value())));
    });
  }

  void read_file(std::function<void(Result<std::filesystem::path>)> handler);

  /// Send |data| followed by the buffers in |buff|, which the peer receives as one attached
  /// buffer. The memory of |buff| must stay valid until the handler is called.
  void write_data(Data                                         data,
                  inplace_vector<boost::asio::const_buffer, 2> buff,
                  std::function<void(Result<void>)>            handler) {
    assert(handler);
    write_data_internal(std::move(data), std::move(buff), std::move(handler));
  }

  void write_data(
      Data data, std::function<void(Result<void>)> handler = [](Result<void>) {}) {
    assert(handler);
    write_data_internal(std::move(data), {}, std::move(handler));
  }

  void write_file(std::filesystem::path file, std::function<void(Result<void>)> handler);

  /// Delay the writes by 5 seconds (testing).
  void write_test_delay();

  void request_peer_status() { async_write_status(true); }

  void shutdown(boost::system::error_code const& ec,
                std::source_location             loc = std::source_location::current());
  void shutdown(std::source_location loc = std::source_location::current());

  bool closed() const { return closed_.test(std::memory_order_relaxed); }

  /// Function called when the connection is getting shutdown.
  virtual void on_shutdown();

  boost::asio::io_context& io_context();

  void set_log_prefix(string prefix) {
    std::unique_lock<std::mutex> lk{log_prefix_mtx_};
    log_prefixes_.emplace_back(std::move(prefix));
    log_prefix_.store(log_prefixes_.back().c_str());
  }
  char const* log_prefix() const { return log_prefix_.load(std::memory_order_relaxed); }

  bool ssl_enabled() const { return socket_.index() == 1; }

 private:
  static constexpr unsigned kHeaderWords = 6U;

  using ExtraBuffers = inplace_vector<boost::asio::const_buffer, 2>;

  auto counter() const {
    return std::visit([](auto&& s) { return s.counter(); }, socket_);
  }

  void breakpoint(char const*);

  void enqueue_timeout_check(unsigned times = 0, uint64_t last_counter = 0);

  void write_data_internal(Data data, ExtraBuffers buff, std::function<void(Result<void>)> handler);

  /// Elements are wrapped in non-template structs so std::expected is not an ADL-associated
  /// class of the std::deque iterators. With C++26, libc++'s constrained std::expected operator==
  /// would otherwise make the iterator comparison in ~deque recurse infinitely.
  template <typename M>
  struct ReadQ {
    struct Elem {
      Result<M> v;
    };
    struct Handler {
      std::function<void(Result<M>)> f;
    };

    std::mutex          mtx;
    std::deque<Elem>    queue;
    std::deque<Handler> handlers;
  };

  using ReadData = std::tuple<Data, Bytes>;

  ReadQ<ReadData>              read_data_q_;
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
    kInsertTestDelay,
  };

  /// Data and the attached buffers.
  using WriteData = std::tuple<Data, ExtraBuffers>;

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

  void do_write_file(shared_ptr<Connection> self, int fd, size_t size);

  uint32_t recv_packet_ = 0;
  uint32_t sent_packet_ = 0;

  std::array<uint32_t, kHeaderWords> read_header_{};
  std::array<uint32_t, kHeaderWords> write_header_{};

  void process_header();
  void do_read_header();

  void cleanup(std::error_code ec, int fd, std::filesystem::path const& file);
  void do_read_file(shared_ptr<Connection> self,
                    int                    fd,
                    uint64_t               length,
                    std::filesystem::path  file);
  void do_read_file(uint64_t length);

  void do_read_data(uint32_t data_size, uint32_t buff_size, uint32_t data_chk, uint32_t buff_chk);

  void shutdown(shared_ptr<Connection> self);
  void do_shutdown(ssl_socket&);
  void do_shutdown(tcp_socket&);

  // The frame header words are little-endian on the wire, so that peers of different byte orders
  // can talk, at no cost on little-endian machines.
  static uint32_t ntoh(uint32_t t) { return boost::endian::little_to_native(t); }
  static uint32_t hton(uint32_t t) { return boost::endian::native_to_little(t); }

  void clear(std::atomic_flag& f) { f.clear(std::memory_order_relaxed); }
  bool test(std::atomic_flag& f) { return f.test(std::memory_order_relaxed); }
  bool test_and_set(std::atomic_flag& f) { return f.test_and_set(std::memory_order_relaxed); }

  template <typename MutableBuffer, typename Handler>
  void async_read(MutableBuffer const& buff, Handler handler) {
    if (closed())
      return boost::asio::post(io_context(), [h = std::move(handler)]() mutable {
        h(boost::asio::error::connection_aborted, 0);
      });

    if (test_and_set(outstanding_read_req_)) [[unlikely]]
      breakpoint("more than one outstanding read requests");

    switch (socket_.index()) {
      case 0:
        boost::asio::async_read(std::get<0>(socket_), buff, std::move(handler));
        break;

      case 1:
        boost::asio::async_read(
            std::get<1>(socket_), buff, boost::asio::bind_executor(strand_, std::move(handler)));
        break;
    }
  }

  template <typename ConstBuffer, typename Handler>
  void async_write(ConstBuffer const& buff, Handler handler) {
    if (closed())
      return boost::asio::post(io_context(), [h = std::move(handler)]() mutable {
        h(boost::asio::error::connection_aborted, 0);
      });

    if (test_and_set(outstanding_write_req_)) [[unlikely]]
      breakpoint("more than one outstanding write requests");

    switch (socket_.index()) {
      case 0:
        boost::asio::async_write(std::get<0>(socket_), buff, std::move(handler));
        break;

      case 1:
        boost::asio::async_write(
            std::get<1>(socket_), buff, boost::asio::bind_executor(strand_, std::move(handler)));
        break;
    }
  }

  // Identity of the remote peer.
  string const peer_;

  boost::asio::io_context& io_context_;

  socket_type                     socket_;
  boost::asio::io_context::strand strand_;

  // File transfers
  static constexpr unsigned kFileTransferSize = kPageSize * 8;

  unique_ptr<std::array<char, kFileTransferSize + 4U>> send_file_buff_;  ///< Staging to send.
  unique_ptr<std::array<char, kFileTransferSize + 4U>> recv_file_buff_;  ///< Staging to receive.

  struct Timer : boost::asio::steady_timer {
   public:
    Timer(boost::asio::io_context& io_context) : boost::asio::steady_timer(io_context) {}

    template <typename Handler>
    void async_wait(std::chrono::milliseconds timeout, Handler&& handler) {
      auto lk = std::unique_lock{mtx_};
      boost::asio::steady_timer::expires_after(timeout);
      boost::asio::steady_timer::async_wait(std::forward<Handler>(handler));
    }

    // Intentionally hides steady_timer::cancel to serialize it with async_wait.
    size_t cancel() {  // NOLINT(bugprone-derived-method-shadowing-base-method)
      auto lk = std::unique_lock{mtx_};
      return boost::asio::steady_timer::cancel();
    }

   private:
    mutable std::mutex mtx_;
  };

  /// Check the socket's counter every timeout period, if the counter has not
  /// changed in so many invocations, close the socket.
  Timer                           timeout_timer_;
  std::chrono::milliseconds const timeout_;
  unsigned const                  times_;

  // Delay timer used for testing.
  Timer delay_timer_;

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

/// If the timeout is 5 seconds, check every 5/10 seconds, no more often then 100 milliseconds and
/// not delayed by more than 1 second.
inline std::tuple<std::chrono::milliseconds, unsigned> timeout_and_times(
    std::chrono::milliseconds t) {
  if (t.count() == 0)
    return std::tuple<std::chrono::milliseconds, unsigned>{0, 0};

  auto timeout =
      std::min(std::chrono::milliseconds(1000), std::max(std::chrono::milliseconds(100), t / 10));
  auto times = std::max<unsigned>(10U, static_cast<unsigned>(t.count() / timeout.count()));
  return std::tuple<std::chrono::milliseconds, unsigned>{timeout, times};
}
