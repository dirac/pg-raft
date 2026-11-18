#pragma once
#include "src/base/base.h"

struct StandbyStatus {
  uint64_t written_lsn;
  uint64_t flushed_lsn;
  uint64_t applied_lsn;
};

// The name of the WAL segment file holding |lsn| on |tli|, as PostgreSQL's XLogFileName: the
// timeline, then the segment number split into the 4 GiB "log id" and the segment within it
// (not the segment number's high and low 32 bits: 1/00000000 is 000000010000000100000000).
std::string wal_segment_name(uint32_t tli, uint64_t lsn, uint32_t segment_size);

// Asio-based WAL sender.  Handles one physical replication connection.
// Reads WAL from |wal_dir| as advance() notifications arrive.
//
// Timelines: |timeline| is the timeline to start from. Later timelines are learned from their
// history files (`%08X.history`) in |wal_dir|, which raft writes there when it applies a
// TimelineOp. The sender answers TIMELINE_HISTORY from them, streams a historic timeline up to its
// switch point, then ends the stream with CopyDone and the (next_tli, next_tli_startpos) row, so
// that the standby continues on the next timeline. Bytes before a timeline's start point are read
// from the parent timeline's segment: on promotion PostgreSQL copies them into the new timeline's
// first segment through a temporary file, which the FUSE layer doesn't replicate.
class WalSender : public std::enable_shared_from_this<WalSender> {
 public:
  using stream_socket = asio::generic::stream_protocol::socket;
  using StatusCb      = std::function<void(StandbyStatus const&)>;

  static shared_ptr<WalSender> create(
      stream_socket          socket,
      std::filesystem::path  wal_dir,
      uint64_t               system_id,
      uint32_t               timeline,
      uint64_t               flush_lsn,
      StatusCb               status_cb        = {},
      uint32_t               wal_segment_size = 16u * 1024u * 1024u);

  void start();

  // Notify that WAL has been flushed (committed) up to flush_lsn, on flush_lsn.timeline.
  // Thread-safe.
  void advance(LogSeqNumber flush_lsn);

  void shutdown();

 private:
  enum class Phase {
    kStartup,
    kCommand,
    kStreaming,
    kEndOfStream,  // Sent CopyDone at the end of a historic timeline, waiting for the standby's.
  };

  // A timeline's range of WAL in the history of the latest timeline: [begin, end).
  struct TimelineRange {
    uint32_t tli;
    uint64_t begin;
    uint64_t end;  // kOpenEnd for the latest timeline
  };
  static constexpr uint64_t kOpenEnd = ~uint64_t{0};

  WalSender(stream_socket socket, std::filesystem::path wal_dir,
            uint64_t system_id, uint32_t timeline, uint64_t flush_lsn,
            StatusCb status_cb, uint32_t wal_segment_size);

  shared_ptr<WalSender> self() { return shared_from_this(); }

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
  void do_advance(LogSeqNumber flush_lsn);
  void send_wal();
  void end_of_timeline();
  void end_of_stream_reply();
  void schedule_keepalive();

  // Timelines.
  void                   load_timelines(uint32_t hint = 0);
  uint32_t               latest_tli() const { return history_.back().tli; }
  std::optional<size_t>  find_tli(uint32_t tli) const;
  uint64_t               committed_limit(size_t idx) const;
  static std::optional<vector<TimelineRange>> parse_history(string const& content,
                                                            uint32_t      tli);

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

  stream_socket            socket_;
  asio::io_context::strand strand_;
  std::filesystem::path    wal_dir_;
  uint64_t                 system_id_;
  vector<TimelineRange>    history_;     // the latest timeline's history, oldest first
  LogSeqNumber             flush_;       // committed WAL, see advance()
  uint32_t                 stream_tli_{0};  // the timeline being streamed
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
