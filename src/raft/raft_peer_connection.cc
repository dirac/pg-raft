#include "src/raft/raft_peer_connection.h"

void RaftPeerConnection::send_ping(RaftLog const& rlog) {
  std::unique_lock<std::mutex> lk{mtx_};

  RaftTerm current_term{0};
  if (auto r = rlog.current_term(); r)
    current_term = r.value();
  else
    return system_error(elog(r.error(), "Failed to get current term"));

  return send_request(std::move(lk),
                      RaftRequest{
                          .term = current_term,
                      },
                      true);
}

void RaftPeerConnection::send_request_vote(RaftLog const& rlog, RaftTerm candidate_term) {
  std::unique_lock<std::mutex> lk{mtx_};

  std::tuple<RaftTerm, RaftIndex> last_entry;
  if (auto r = rlog.last_entry(); r)
    last_entry = r.value();
  else
    return system_error(elog(r.error(), "Failed to get last entry"));

  return send_request(std::move(lk),
                      RaftRequest{
                          .term = candidate_term,
                          .request =
                              RequestVote{
                                  .last_entry = last_entry,
                              },
                      },
                      true);
}

void RaftPeerConnection::send_request_election(RaftLog const& rlog, RaftTerm leadership_term) {
  std::unique_lock<std::mutex> lk{mtx_};

  std::tuple<RaftTerm, RaftIndex> last_entry;
  if (auto r = rlog.last_entry(); r)
    last_entry = r.value();
  else
    return system_error(elog(r.error(), "Failed to get last entry"));

  return send_request(std::move(lk),
                      RaftRequest{
                          .term = leadership_term,
                          .request =
                              RequestElection{
                                  .last_entry = last_entry,
                              },
                      },
                      true);
}

void RaftPeerConnection::set_leader_term(RaftLog const&                      rlog,
                                         RaftTerm                            leadership_term,
                                         std::unique_lock<std::mutex> const& lk) {
  // We have a new leader.
  if (leader_term_ != leadership_term) {
    RaftIndex current_idx;
    if (auto r = rlog.last_entry(); r)
      current_idx = std::get<RaftIndex>(r.value());
    else
      return system_error(elog(r.error(), "Failed to get last entry"));

    leader_term_ = leadership_term;
    last_send_   = 0U;
    set_next_idx(current_idx + 1U, lk);
  }
}

void RaftPeerConnection::send_append_entries(std::unique_lock<std::mutex> lk,
                                             RaftLog const&               rlog,
                                             RaftTerm                     leadership_term,
                                             RaftIndex                    commit_idx) {
  set_leader_term(rlog, leadership_term, lk);

  RaftIndex current_idx{0};
  if (auto r = rlog.last_entry(); r)
    current_idx = std::get<RaftIndex>(r.value());
  else
    return system_error(elog(r.error(), "Failed to get last entry"));

  if ((next_idx_ >= current_idx) && (epoch_msec() - last_send_) < (options_.election_timeout >> 2))
    return;

  AppendEntries append_entries;
  bool          respond = true;

  append_entries.commit_idx   = commit_idx;
  append_entries.prev_log_idx = next_idx_ - 1U;

  if (auto r = rlog.get_term(append_entries.prev_log_idx); !r)
    return system_error(elog(r.error(), "Failed to get_term at {}", append_entries.prev_log_idx));

  else {
    std::optional<RaftTerm> term = r.value();
    if (term)
      append_entries.prev_log_term = *term;
    else
      return send_install_snapshot(std::move(lk), rlog, leadership_term);
  }

  if (auto r = rlog.entries_from(next_idx_, options_.max_entries); !r)
    return system_error(elog(r.error(), "Failed to entries_from at {}", next_idx_));
  else {
    std::optional<vector<Entry>> entries = std::move(r.value());
    if (entries) {
      append_entries.entries = std::move(entries.value());
      respond                = trim(append_entries.entries);
    } else {
      return send_install_snapshot(std::move(lk), rlog, leadership_term);
    }
  }
  next_idx_ = next_idx_ + append_entries.entries.size();

  auto data = mem_blocks(append_entries.entries);
  return send_request(std::move(lk),
                      RaftRequest{
                          .term    = leadership_term,
                          .request = std::move(append_entries),
                          .data    = data,
                      },
                      respond);
}

void RaftPeerConnection::send_install_snapshot(std::unique_lock<std::mutex> lk,
                                               RaftLog const&               rlog,
                                               RaftTerm                     leadership_term) {
  set_leader_term(rlog, leadership_term, lk);

  if (auto r = rlog.latest_entries(options_.snapshot_entries_count); !r)
    return system_error(elog(r.error(), "failed in getting latest entries"));
  else {
    auto [idx, entries] = std::move(r.value());

    next_idx_ = idx + 1U;
    trim(entries);

    std::reverse(entries.begin(), entries.end());
    idx -= entries.size();

    auto data = mem_blocks(entries);
    send_request(std::move(lk),
                 RaftRequest{
                     .term = leadership_term,
                     .request =
                         InstallSnapshot{
                             .log_idx = idx,
                             .entries = std::move(entries),
                         },
                     .data = data,
                 },
                 true);
  }
}

inplace_vector<MemBlock, 2> RaftPeerConnection::mem_blocks(vector<Entry> const& entries) {
  map<LogSeqNumber, std::tuple<off_t, off_t>> regions;
  for (auto const& e : entries) {
    if (auto const* wal_cmd = std::get_if<WalCommand>(&e.cmd)) {
      if (wal_cmd->segment && std::holds_alternative<WriteOp>(wal_cmd->command)) {
        auto pos = regions.find(wal_cmd->segment.value());
        if (pos == regions.end())
          pos = regions.emplace(wal_cmd->segment.value(), std::make_tuple(0, 0)).first;

        WriteOp op  = std::get<WriteOp>(wal_cmd->command);
        auto [s, e] = pos->second;
        s           = std::min(op.offset, s);
        e           = std::max(op.offset + static_cast<off_t>(op.size), e);
        pos->second = std::make_tuple(s, e);
      }
    }
  }

  inplace_vector<MemBlock, 2> result;
  for (auto [lsn, reg] : regions)
    result.emplace_back(MemBlock{
        .lsn    = lsn,
        .offset = std::get<0>(reg),
        .size   = static_cast<size_t>(std::get<1>(reg) - std::get<0>(reg)),
    });
  return result;
}

bool RaftPeerConnection::trim(vector<Entry>& entries) {
  bool respond = entries.size() > 10;

  set<LogSeqNumber> segments;

  for (auto iter = std::begin(entries); iter != std::end(entries); ++iter) {
    if (auto const* wal_cmd = std::get_if<WalCommand>(&(iter->cmd))) {
      if (wal_cmd->segment && std::holds_alternative<WriteOp>(wal_cmd->command) &&
          segments.insert(wal_cmd->segment.value()).second) {
        if (segments.size() == 3) {
          entries.erase(iter, entries.end());
          return respond;
        }
      }
      respond = respond || std::holds_alternative<FlushOp>(wal_cmd->command);
    }
  }
  return respond;
}

void RaftPeerConnection::send_request(std::unique_lock<std::mutex> lk,
                                      RaftRequest                  request,
                                      bool                         respond) {
  set_info_and_node_id(request, respond);

  last_send_ = epoch_msec();

  if (request.info.log)
    log(INFO, "{}{}", log_prefix(), request);

  inplace_vector<asio::const_buffer, 2>     buffers;
  inplace_vector<shared_ptr<WalSegment>, 2> segments;
  for (auto const& block : request.data) {
    if (auto s = segment_; s && s->lsn() == block.lsn) {
      segments.emplace_back(std::move(s));
      buffers.emplace_back(s->const_buffer(block.offset, block.size));
    } else {
      if (auto seg = server_->segment(block.lsn); seg) {
        segments.emplace_back(std::move(seg.value()));
        buffers.emplace_back(s->const_buffer(block.offset, block.size));
      } else
        return system_error(elog(seg.error(), "Unable to fetch the wal segment for {}", block.lsn));
    }
  }
  write_data(to_data(request),
             buffers,
             [this, s = self(), segments = std::move(segments)](Result<void> r) {
               if (!r)
                 shutdown(r.error());
             });

  if (respond)
    read_response();
  lk.unlock();
}

void RaftPeerConnection::response_handler(Result<Data> msg) {
  if (msg.has_error())
    return shutdown(msg.error());

  if (auto r = from_data<RaftResponse>(std::move(msg.value())); !r)
    return shutdown(r.error());
  else {
    /// The request was sent by the server with the following term.
    update_response_seq(r.value());

    RaftTerm   request_term = r.value().info.request_term;
    RaftTerm   current_term = r.value().term;
    RaftNodeId node_id      = r.value().node_id;

    assert(node_id == peer_id());

    if (r.value().info.log) {
      log(INFO, "{}{}", log_prefix(), r.value());
    }

    // round_trip_time = epoch_msec() - r.value().info.epoch;

    std::visit(
        [this, request_term, current_term](auto&& r) {
          if (auto lk = server_->usage_lock(); lk)
            server_->handle_response(
                self<RaftPeerConnection>(), request_term, current_term, std::move(r));
        },
        r.value().response);
  }
}

void RaftPeerConnection::read_response() {
  auto handler = [this, s = self()](Result<Data> msg) mutable { response_handler(std::move(msg)); };
  read_data(std::move(handler));
}
