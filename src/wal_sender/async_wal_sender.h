#pragma once
#include "src/base/base.h"

struct StandbyStatus {
  uint64_t written_lsn;
  uint64_t flushed_lsn;
  uint64_t applied_lsn;
};

// Asio-based WAL sender.  Handles one physical replication connection.
// Reads WAL from |wal_dir| as advance() notifications arrive.
class AsyncWalSender : public std::enable_shared_from_this<AsyncWalSender> {
 public:
  using StatusCb = std::function<void(StandbyStatus const&)>;

  static shared_ptr<AsyncWalSender> create(
      asio::ip::tcp::socket  socket,
      std::filesystem::path  wal_dir,
      uint64_t               system_id,
      uint32_t               timeline,
      uint64_t               flush_lsn,
      StatusCb               status_cb        = {},
      uint32_t               wal_segment_size = 16u * 1024u * 1024u);

  void start();

  // Notify that WAL has been flushed up to flush_lsn.  Thread-safe.
  void advance(uint64_t flush_lsn);

  void shutdown();

 private:
  using tcp = asio::ip::tcp;
  enum class Phase { kStartup, kCommand, kStreaming };

  AsyncWalSender(tcp::socket socket, std::filesystem::path wal_dir,
                 uint64_t system_id, uint32_t timeline, uint64_t flush_lsn,
                 StatusCb status_cb, uint32_t wal_segment_size);

  shared_ptr<AsyncWalSender> self() { return shared_from_this(); }

  // Startup packet (no type byte: length + body).
  void do_read_startup_len();
  void do_read_startup_body(uint32_t total_len);
  void handle_startup_body(Bytes body);

  // Regular command-loop messages.
  void do_read_msg_hdr();
  void do_read_msg_body(char type, uint32_t body_len);
  void handle_msg(char type, Bytes body);

  // CopyBoth streaming messages from standby.
  void do_read_copy_hdr();
  void do_read_copy_body(char type, uint32_t body_len);
  void handle_copy_msg(char type, Bytes const& body);

  // Write side.
  void enqueue_send(Bytes buf);
  void drain_sends();

  // WAL reading and streaming.
  void do_advance(uint64_t flush_lsn);
  void schedule_keepalive();

  void close_socket();

  // Protocol message builders.
  static Bytes build_msg(char type, Bytes body);
  static Bytes auth_ok();
  static Bytes param_status(char const* name, char const* value);
  static Bytes backend_key_data();
  static Bytes ready_for_query();
  static Bytes row_desc(vector<std::pair<string, int32_t>> const& cols);
  static Bytes data_row(vector<std::optional<string>> const& vals);
  static Bytes command_complete(char const* tag);
  static Bytes copy_both_response();
  static Bytes error_response(char const* m);
  Bytes        xlog_data(uint64_t lsn, Bytes const& data) const;
  Bytes        keepalive_msg() const;

  static uint64_t parse_lsn(string const& s);
  static string   format_lsn(uint64_t lsn);
  static int64_t  now_pg_ts();

  static void     put8(Bytes& b, uint8_t v) { b.push_back(v); }
  static void     put16(Bytes& b, uint16_t v);
  static void     put32(Bytes& b, uint32_t v);
  static void     put64(Bytes& b, uint64_t v);
  static void     put_str(Bytes& b, string const& s);
  static uint32_t get32(uint8_t const* p);
  static uint64_t get64(uint8_t const* p);

  tcp::socket              socket_;
  asio::io_context::strand strand_;
  std::filesystem::path    wal_dir_;
  uint64_t                 system_id_;
  uint32_t                 timeline_;
  uint64_t                 flush_lsn_;
  uint64_t                 sent_lsn_{0};
  uint64_t                 end_lsn_;
  uint32_t                 wal_segment_size_;
  StatusCb                 status_cb_;
  Phase                    phase_{Phase::kStartup};
  bool                     shutdown_{false};

  array<uint8_t, 4> startup_len_buf_;
  array<uint8_t, 5> hdr_buf_;
  Bytes             body_buf_;

  std::deque<Bytes> send_queue_;
  Bytes             writing_buf_;
  bool              writing_{false};

  asio::steady_timer keepalive_timer_;
};
