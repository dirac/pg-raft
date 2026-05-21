#pragma once

#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <vector>

struct WalSegment {
  uint64_t start_lsn;
  std::vector<uint8_t> data;
};

struct StandbyStatus {
  uint64_t written_lsn;
  uint64_t flushed_lsn;
  uint64_t applied_lsn;
};

// Drives a single physical streaming-replication connection from a standby.
//
// Call run() after construction; it handles the PostgreSQL startup handshake
// then enters the WAL-streaming loop.  WAL segments are fed via send()
// (thread-safe).  The standby's status reports are forwarded to status_cb.
class WalSender {
 public:
  using StatusCallback = std::function<void(const StandbyStatus&)>;

  // Takes ownership of |fd|.
  // |system_id|  – value from pg_control's system identifier.
  // |timeline|   – current timeline ID.
  // |end_lsn|    – initial end-of-WAL LSN (updated as segments are sent).
  WalSender(int fd, uint64_t system_id, uint32_t timeline, uint64_t end_lsn,
            StatusCallback status_cb = {});
  ~WalSender();

  WalSender(const WalSender&) = delete;
  WalSender& operator=(const WalSender&) = delete;

  // Block until the connection closes or an error occurs.
  bool run();

  // Enqueue a WAL segment for streaming. Thread-safe.
  void send(WalSegment seg);

  // Signal that no more WAL will follow. run() returns after draining.
  void shutdown();

 private:
  bool do_handshake();
  bool handle_identify_system();
  bool handle_start_replication(const std::string& cmd);

  bool streaming_loop(uint64_t start_lsn);
  bool send_xlog_data(const WalSegment& seg);
  bool send_keepalive(bool reply_requested);
  bool process_copy_data(const std::vector<uint8_t>& payload);

  bool recv_startup(std::vector<std::pair<std::string, std::string>>* params);
  bool recv_message(char* type, std::vector<uint8_t>* payload);
  bool send_message(char type, const std::vector<uint8_t>& payload);
  bool recv_raw(void* buf, size_t n);
  bool send_raw(const void* buf, size_t n);

  bool send_auth_ok();
  bool send_parameter_status(const char* name, const char* value);
  bool send_backend_key_data();
  bool send_ready_for_query();
  bool send_row_description(
      const std::vector<std::pair<std::string, int32_t>>& cols);
  bool send_data_row(const std::vector<std::optional<std::string>>& vals);
  bool send_command_complete(const char* tag);
  bool send_copy_both_response();
  bool send_error_response(const char* msg);

  static uint64_t parse_lsn(const std::string& s);
  static std::string format_lsn(uint64_t lsn);
  static void put_u8(std::vector<uint8_t>& b, uint8_t v);
  static void put_u16be(std::vector<uint8_t>& b, uint16_t v);
  static void put_u32be(std::vector<uint8_t>& b, uint32_t v);
  static void put_u64be(std::vector<uint8_t>& b, uint64_t v);
  static void put_i64be(std::vector<uint8_t>& b, int64_t v);
  static void put_str(std::vector<uint8_t>& b, const std::string& s);
  static uint32_t get_u32be(const uint8_t* p);
  static uint64_t get_u64be(const uint8_t* p);
  static int64_t now_pg_timestamp();

  int fd_;
  uint64_t system_id_;
  uint32_t timeline_;
  uint64_t end_lsn_;
  StatusCallback status_cb_;

  // Write a byte to wakeup_[1] from send() to unblock the streaming poll.
  int wakeup_[2] = {-1, -1};

  std::mutex mu_;
  std::queue<WalSegment> queue_;
  bool shutdown_ = false;
};
