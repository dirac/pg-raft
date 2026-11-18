#include "src/raft/raft_peer_connection.h"

#include "src/base/logger.h"

void RaftPeerConnection::send_ping(RaftTerm current_term) {
  std::unique_lock<std::mutex> lk{mtx_};
  return send_request(std::move(lk),
                      RaftRequest{
                          .term = current_term,
                      });
}

void RaftPeerConnection::send_request_vote(Ledger const& ledger, RaftTerm candidate_term) {
  std::unique_lock<std::mutex> lk{mtx_};
  return send_request(std::move(lk),
                      RaftRequest{
                          .term = candidate_term,
                          .request =
                              RequestVote{
                                  .last_entry = ledger.last_entry(),
                              },
                      });
}

void RaftPeerConnection::send_request_election(Ledger const& ledger, RaftTerm leadership_term) {
  std::unique_lock<std::mutex> lk{mtx_};
  return send_request(std::move(lk),
                      RaftRequest{
                          .term = leadership_term,
                          .request =
                              RequestElection{
                                  .last_entry = ledger.last_entry(),
                              },
                      });
}

void RaftPeerConnection::set_leader_term(Ledger const&                       ledger,
                                         RaftTerm                            leadership_term,
                                         std::unique_lock<std::mutex> const& lk) {
  // We have a new leader.
  if (leader_term_ != leadership_term) {
    leader_term_ = leadership_term;
    last_send_   = 0;
    set_next_idx(std::get<RaftIndex>(ledger.last_entry()) + 1U, lk);
  }
}

void RaftPeerConnection::send_append_entries(std::unique_lock<std::mutex> lk,
                                             Ledger const&                ledger,
                                             RaftTerm                     leadership_term,
                                             RaftIndex                    commit_idx) {
  set_leader_term(ledger, leadership_term, lk);

  // An InstallSnapshot in flight: its chunks go one at a time, and AppendEntries follow it.
  if (snapshot_ && install_snapshot() > 0)
    return;
  snapshot_.reset();

  auto const current_idx = std::get<RaftIndex>(ledger.last_entry());
  if ((next_idx_ > current_idx) &&
      (epoch_msec() - last_send_) < (options_.election_timeout_msecs >> 2U))
    return;

  AppendEntries append_entries;
  append_entries.commit_idx   = commit_idx;
  append_entries.prev_log_idx = next_idx_ - 1U;

  // Compacted away: the follower needs a snapshot.
  auto term = ledger.term(append_entries.prev_log_idx);
  if (!term)
    return send_install_snapshot(std::move(lk), ledger, leadership_term);
  append_entries.prev_log_term = *term;

  auto batch = ledger.records(next_idx_, options_.max_entries, options_.max_request_bytes);
  if (!batch)
    return system_error(elog(batch.error(), "Failed to read the records from {}", next_idx_));
  if (!*batch) {
    next_idx_ = current_idx + 1U;
    return send_install_snapshot(std::move(lk), ledger, leadership_term);
  }
  append_entries.n = (*batch)->count();
  next_idx_        = next_idx_ + append_entries.n;

  // The records, as the ledger files hold them (at most two buffers: one per file).
  auto attached = make_shared<vector<Bytes>>();
  for (auto& buf : (*batch)->buffers)
    attached->push_back(std::move(buf));

  return send_request(std::move(lk),
                      RaftRequest{
                          .term    = leadership_term,
                          .request = std::move(append_entries),
                      },
                      attached);
}

void RaftPeerConnection::send_install_snapshot(std::unique_lock<std::mutex> lk,
                                               Ledger const&                ledger,
                                               RaftTerm                     leadership_term) {
  set_leader_term(ledger, leadership_term, lk);

  // The first ledger file: the leader keeps every entry from the latest backup on, so a follower
  // that restores that backup gets the rest of the WAL from here.
  auto file = ledger.snapshot_file();
  if (!file) {
    // An empty log: the follower matches at index 0.
    next_idx_ = RaftIndex{1};
    return send_append_entries(std::move(lk), ledger, leadership_term, RaftIndex{0});
  }
  log(INFO, "{}InstallSnapshot of {} ({} bytes)", log_prefix(), file->first, file->size);
  snapshot_ = Snapshot{.file = *file, .sent = 0};
  send_snapshot_chunk(std::move(lk), ledger, leadership_term);
}

void RaftPeerConnection::send_snapshot_chunk(std::unique_lock<std::mutex> lk,
                                             Ledger const&                ledger,
                                             RaftTerm                     leadership_term) {
  if (!snapshot_)
    return send_install_snapshot(std::move(lk), ledger, leadership_term);

  auto const file  = snapshot_->file;
  auto const sent  = snapshot_->sent;
  auto const size  = std::min(options_.max_request_bytes, file.size - sent);
  auto       bytes = ledger.read_snapshot(file.first, sent, size);
  if (!bytes)
    return system_error(elog(bytes.error(), "Failed to read the snapshot {}", file.first));
  if (!*bytes) {
    // Compacted meanwhile: the new first file.
    snapshot_.reset();
    return send_install_snapshot(std::move(lk), ledger, leadership_term);
  }

  snapshot_->sent += size;
  // After the last chunk the follower answers with its last index, and AppendEntries continue.
  auto attached = make_shared<vector<Bytes>>();
  attached->push_back(std::move(**bytes));
  send_request(std::move(lk),
               RaftRequest{
                   .term    = leadership_term,
                   .request = InstallSnapshot{.first = file.first, .offset = sent, .size = file.size},
               },
               attached);
}

void RaftPeerConnection::send_request(std::unique_lock<std::mutex>     lk,
                                      RaftRequest                      request,
                                      shared_ptr<vector<Bytes>> const& attached) {
  set_info_and_node_id(request);

  last_send_ = epoch_msec();

  if (request.info.log)
    log(INFO, "{}{}", log_prefix(), request);

  auto buffers = inplace_vector<asio::const_buffer, 2>{};
  if (attached)
    for (auto const& b : *attached)
      if (!b.empty())
        buffers.emplace_back(b.data(), b.size());

  write_data(to_data(request), buffers, [this, s = self(), attached](Result<void> r) {
    if (!r)
      shutdown(r.error());
  });
  read_response();

  lk.unlock();
}

void RaftPeerConnection::response_handler(Result<Data> msg) {
  if (msg.has_error())
    return shutdown(msg.error());

  auto r = from_data<RaftResponse>(msg.value());
  if (!r)
    return shutdown(r.error());

  /// The request was sent by the server with the following term.
  update_response_seq(r.value());

  RaftTerm   request_term = r.value().info.request_term;
  RaftTerm   current_term = r.value().term;
  RaftNodeId node_id      = r.value().node_id;

  assert(node_id == peer_id());
  (void)node_id;

  if (r.value().info.log)
    log(INFO, "{}{}", log_prefix(), r.value());

  std::visit(
      [this, request_term, current_term](auto&& v) {
        if (auto s = server_.lock(); s)
          if (auto lk = s->usage_lock(); lk)
            s->handle_response(self<RaftPeerConnection>(),
                               request_term,
                               current_term,
                               std::forward<decltype(v)>(v));
      },
      std::move(r.value().response));
}

void RaftPeerConnection::read_response() {
  read_data([this, s = self()](Result<Data> msg) mutable { response_handler(std::move(msg)); });
}
