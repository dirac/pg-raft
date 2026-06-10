#pragma once

#include "src/base/base.h"

using Row  = vector<std::optional<string>>;
using Rows = vector<Row>;

// Handle returned by PgClient::prepare(); pass to async_exec.
struct PgStmt {
  string name;
};

class PgClient : public std::enable_shared_from_this<PgClient> {
 public:
  static Result<shared_ptr<PgClient>> connect(uint16_t port);

  Result<PgStmt> prepare(string const& sql);

  void async_exec(PgStmt const& stmt, std::function<void(Result<Rows>)> cb);

  void close();

 private:
  using tcp    = asio::ip::tcp;
  using ExecCb = std::function<void(Result<Rows>)>;

  struct Req {
    string name;
    ExecCb cb;
  };

  explicit PgClient(tcp::socket socket);

  // Synchronous helpers used by connect() and prepare().
  Result<void>                   sync_write(Bytes const& buf);
  Result<std::pair<char, Bytes>> sync_read_msg();
  Result<void>                   sync_drain();

  // Async exec chain.
  void enqueue(Req r);
  void start_exec(shared_ptr<PgClient> self);
  void do_send(shared_ptr<PgClient> self);
  void do_read_hdr(shared_ptr<PgClient> self);
  void do_read_body(shared_ptr<PgClient> self, char type, uint32_t body_len);
  void on_msg(shared_ptr<PgClient> self, char type);
  void finish_exec(shared_ptr<PgClient> self, Result<Rows> result);

  tcp::socket              socket_;
  asio::io_context::strand strand_;
  int                      next_id_{0};

  // Exec state — accessed only from strand.
  ExecCb            exec_cb_;
  Rows              rows_;
  Bytes             send_buf_;
  array<uint8_t, 5> hdr_;
  Bytes             body_;
  bool              exec_failed_{false};

  // Pending request queue — protected by mu_.
  std::mutex      mu_;
  std::deque<Req> queue_;
  bool            running_{false};
};
