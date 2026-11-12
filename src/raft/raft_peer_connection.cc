#include "src/raft/raft_peer_connection.h"

#include "src/base/logger.h"

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
                      });
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
                      });
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
                      });
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
    last_send_   = 0;
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

  if ((next_idx_ > current_idx) &&
      (epoch_msec() - last_send_) < (options_.election_timeout_msecs >> 2U))
    return;

  AppendEntries append_entries;

  append_entries.commit_idx   = commit_idx;
  append_entries.prev_log_idx = next_idx_ - 1U;

  auto term_result = rlog.get_term(append_entries.prev_log_idx);
  if (!term_result)
    return system_error(
        elog(term_result.error(), "Failed to get_term at {}", append_entries.prev_log_idx));

  std::optional<RaftTerm> term = term_result.value();
  if (term)
    append_entries.prev_log_term = *term;
  else
    return send_install_snapshot(std::move(lk), rlog, leadership_term);

  auto entries_result = rlog.entries_from(next_idx_, options_.max_entries);
  if (!entries_result)
    return system_error(elog(entries_result.error(), "Failed to entries_from at {}", next_idx_));

  std::optional<vector<Entry>> entries = std::move(entries_result.value());
  if (entries) {
    append_entries.entries = std::move(entries.value());
    trim(append_entries.entries);
  } else {
    next_idx_ = current_idx + 1U;
    return send_install_snapshot(std::move(lk), rlog, leadership_term);
  }
  next_idx_ = next_idx_ + append_entries.entries.size();

  auto data = mem_blocks(append_entries.entries, /*full_wal_segment=*/false);
  return send_request(std::move(lk),
                      RaftRequest{
                          .term    = leadership_term,
                          .request = std::move(append_entries),
                          .data    = std::move(data),
                      });
}

void RaftPeerConnection::send_install_snapshot(std::unique_lock<std::mutex> lk,
                                               RaftLog const&               rlog,
                                               RaftTerm                     leadership_term) {
  set_leader_term(rlog, leadership_term, lk);

  auto r = rlog.latest_entries(options_.snapshot_entries);
  if (!r)
    return system_error(elog(r.error(), "Failed in getting the latest entries"));

  auto [idx, entries] = std::move(r.value());

  // The latest entries (the last first) that write to at most two segments.
  trim(entries);
  std::ranges::reverse(entries);

  next_idx_ = idx + 1U;

  auto server   = server_.lock();
  auto timeline = server ? server->latest_timeline() : std::nullopt;
  auto state    = server ? server->applied_state() : RaftState{};

  auto data = mem_blocks(entries, /*full_wal_segment=*/true);
  send_request(std::move(lk),
               RaftRequest{
                   .term = leadership_term,
                   .request =
                       InstallSnapshot{
                           .log_idx  = idx - entries.size(),
                           .entries  = std::move(entries),
                           .state    = state,
                           .timeline = std::move(timeline),
                       },
                   .data = std::move(data),
               });
}

inplace_vector<MemBlock, 2> RaftPeerConnection::mem_blocks(vector<Entry> const& entries,
                                                           bool                 full_wal_segment) {
  map<LogSeqNumber, std::tuple<off_t, off_t>> regions;
  for (auto const& e : entries) {
    auto const& cmd = e.cmd;
    if (!cmd.segment)
      continue;

    if (auto const* op = std::get_if<WriteOp>(&cmd.command); op) {
      auto const end = op->offset + static_cast<off_t>(op->size);

      auto pos = regions.find(cmd.segment.value());
      if (pos == regions.end()) {
        regions.emplace(cmd.segment.value(),
                        full_wal_segment ? std::make_tuple(off_t{0}, off_t{kWalSegmentSize})
                                         : std::make_tuple(op->offset, end));
      } else {
        auto& [s, f] = pos->second;
        s            = std::min(op->offset, s);
        f            = std::max(end, f);
      }
    }
  }
  assert(regions.size() <= 2);

  inplace_vector<MemBlock, 2> result;
  for (auto const& [lsn, reg] : regions)
    result.emplace_back(MemBlock{
        .lsn    = lsn,
        .offset = std::get<0>(reg),
        .size   = static_cast<size_t>(std::get<1>(reg) - std::get<0>(reg)),
    });
  return result;
}

void RaftPeerConnection::trim(vector<Entry>& entries) {
  set<LogSeqNumber> segments;

  for (auto iter = std::begin(entries); iter != std::end(entries); ++iter) {
    auto const& cmd = iter->cmd;
    if (cmd.segment && std::holds_alternative<WriteOp>(cmd.command) &&
        segments.insert(cmd.segment.value()).second && segments.size() == 3) {
      entries.erase(iter, entries.end());
      return;
    }
  }
}

void RaftPeerConnection::send_request(std::unique_lock<std::mutex> lk, RaftRequest request) {
  set_info_and_node_id(request);

  last_send_ = epoch_msec();

  if (request.info.log)
    log(INFO, "{}{}", log_prefix(), request);

  // The WAL bytes, copied from the segments' mmaps: PostgreSQL keeps writing to the segments (and
  // rewrites the last page), and the bytes sent must be the ones the frame's checksum is of.
  auto bytes = make_shared<Bytes>();
  if (!request.data.empty()) {
    auto server = server_.lock();
    if (!server)
      return shutdown();

    bytes->reserve(std::accumulate(std::begin(request.data),
                                   std::end(request.data),
                                   size_t{0},
                                   [](size_t r, MemBlock const& b) { return r + b.size; }));
    for (auto const& block : request.data) {
      auto seg = server->segment(block.lsn);
      if (!seg)
        return system_error(elog(seg.error(), "Unable to fetch the wal segment for {}", block.lsn));

      auto const* p = seg.value()->mapping() + block.offset;
      bytes->insert(bytes->end(), p, p + block.size);
    }
  }

  auto buffers = inplace_vector<asio::const_buffer, 2>{};
  if (!bytes->empty())
    buffers.emplace_back(bytes->data(), bytes->size());

  write_data(to_data(request), buffers, [this, s = self(), bytes](Result<void> r) {
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
