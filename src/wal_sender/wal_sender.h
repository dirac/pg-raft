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
// Call Run() after construction; it handles the PostgreSQL startup handshake
// then enters the WAL-streaming loop.  WAL segments are fed via Send()
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
  bool Run();

  // Enqueue a WAL segment for streaming. Thread-safe.
  void Send(WalSegment seg);

  // Signal that no more WAL will follow. Run() returns after draining.
  void Shutdown();

 private:
  bool DoHandshake();
  bool HandleIdentifySystem();
  bool HandleStartReplication(const std::string& cmd);

  bool StreamingLoop(uint64_t start_lsn);
  bool SendXLogData(const WalSegment& seg);
  bool SendKeepalive(bool reply_requested);
  bool ProcessCopyData(const std::vector<uint8_t>& payload);

  bool RecvStartup(std::vector<std::pair<std::string, std::string>>* params);
  bool RecvMessage(char* type, std::vector<uint8_t>* payload);
  bool SendMessage(char type, const std::vector<uint8_t>& payload);
  bool RecvRaw(void* buf, size_t n);
  bool SendRaw(const void* buf, size_t n);

  bool SendAuthOk();
  bool SendParameterStatus(const char* name, const char* value);
  bool SendBackendKeyData();
  bool SendReadyForQuery();
  bool SendRowDescription(
      const std::vector<std::pair<std::string, int32_t>>& cols);
  bool SendDataRow(const std::vector<std::optional<std::string>>& vals);
  bool SendCommandComplete(const char* tag);
  bool SendCopyBothResponse();
  bool SendErrorResponse(const char* msg);

  static uint64_t ParseLSN(const std::string& s);
  static std::string FormatLSN(uint64_t lsn);
  static void PutU8(std::vector<uint8_t>& b, uint8_t v);
  static void PutU16BE(std::vector<uint8_t>& b, uint16_t v);
  static void PutU32BE(std::vector<uint8_t>& b, uint32_t v);
  static void PutU64BE(std::vector<uint8_t>& b, uint64_t v);
  static void PutI64BE(std::vector<uint8_t>& b, int64_t v);
  static void PutStr(std::vector<uint8_t>& b, const std::string& s);
  static uint32_t GetU32BE(const uint8_t* p);
  static uint64_t GetU64BE(const uint8_t* p);
  static int64_t NowPgTimestamp();

  int fd_;
  uint64_t system_id_;
  uint32_t timeline_;
  uint64_t end_lsn_;
  StatusCallback status_cb_;

  // Write a byte to wakeup_[1] from Send() to unblock the streaming poll.
  int wakeup_[2] = {-1, -1};

  std::mutex mu_;
  std::queue<WalSegment> queue_;
  bool shutdown_ = false;
};
