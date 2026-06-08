#include "src/raft/raft_peer_connection.h"

#include "src/base/cert_functions.h"

Logger RaftPeerConnection::logger{"raft-peer-conn"};

void RaftPeerConnection::send_ping(RaftLog const& rlog) {
  std::unique_lock<std::mutex> lk{mtx_};

  RaftTerm current_term{0};
  if (auto r = rlog.current_term(); r)
    current_term = r.value();
  else
    return system_error(ERROR_MSG(r.error(), "Failed to get current term"));

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
    return system_error(ERROR_MSG(r.error(), "Failed to get last entry"));

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
    return system_error(ERROR_MSG(r.error(), "Failed to get last entry"));

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
      return system_error(ERROR_MSG(r.error(), "Failed to get last entry"));

    leader_term_ = leadership_term;
    last_send_   = 0U;
    set_next_idx(current_idx + 1U, lk);
  }
}

void RaftPeerConnection::send_append_entries(std::unique_lock<std::mutex> lk,
                                             RaftLog const&               rlog,
                                             RaftTerm                     leadership_term,
                                             RaftIndex                    commit_idx,
                                             shared_ptr<Snapshot>         snapshot) {
  set_leader_term(rlog, leadership_term, lk);

  RaftIndex current_idx{0};
  if (auto r = rlog.last_entry(); r)
    current_idx = std::get<RaftIndex>(r.value());
  else
    return system_error(ERROR_MSG(r.error(), "Failed to get last entry"));

  if ((next_idx_ >= current_idx) && (epoch_msec() - last_send_) < (options_.election_timeout >> 2))
    return;

  AppendEntries append_entries;

  append_entries.commit_idx   = commit_idx;
  append_entries.prev_log_idx = next_idx_ - 1U;

  if (auto r = rlog.get_term(append_entries.prev_log_idx); !r)
    return system_error(ERROR_MSG(
        r.error(), fn::format("Failed to get_term at {}", INT(append_entries.prev_log_idx))));

  else {
    std::optional<RaftTerm> term = r.value();
    if (term)
      append_entries.prev_log_term = *term;
    else
      return send_install_snapshot(std::move(lk), rlog, leadership_term, std::move(snapshot));
  }

  if (auto r = rlog.entries_from(next_idx_, options_.max_entries); !r)
    return system_error(
        ERROR_MSG(r.error(), fn::format("Failed to entries_from at {}", INT(next_idx_))));
  else {
    std::optional<vector<Entry>> entries = std::move(r.value());
    if (entries)
      append_entries.entries = std::move(entries.value());
    else {
      next_idx_ = current_idx + 1U;
      return send_install_snapshot(std::move(lk), rlog, leadership_term, std::move(snapshot));
    }
  }
  next_idx_ = next_idx_ + append_entries.entries.size();
  return send_request(std::move(lk),
                      RaftRequest{
                          .term    = leadership_term,
                          .request = std::move(append_entries),
                      });
}

void RaftPeerConnection::send_install_snapshot(std::unique_lock<std::mutex> lk,
                                               RaftLog const&               rlog,
                                               RaftTerm                     leadership_term,
                                               shared_ptr<Snapshot>         snapshot) {
  set_leader_term(rlog, leadership_term, lk);

  std::filesystem::path file;
  string                checksum;
  size_t                size;

  if (auto r = snapshot->details(rlog); !r)
    return system_error(ERROR_MSG(r.error(), "Failed to get the snapshot file"));
  else
    std::tie(file, checksum, size) = r.value();

  send_request(std::move(lk),
               RaftRequest{
                   .term = leadership_term,
                   .request =
                       InstallSnapshot{
                           .size     = size,
                           .checksum = checksum,
                       },
               },
               std::move(file),
               std::move(snapshot));
}

void RaftPeerConnection::send_request(std::unique_lock<std::mutex>         lk,
                                      RaftRequest                          request,
                                      std::optional<std::filesystem::path> file,
                                      shared_ptr<Snapshot>                 snapshot) {
  set_info_and_node_id(request);

  last_send_ = epoch_msec();

  if (request.info.log) {
    LOG_INFO << log_prefix() << request;
  }

  if (file) {
    write_data(to_data(request), [this, s = self()](Result<void> r) {
      if (!r)
        shutdown(r.error());
    });
    write_file(std::move(file.value()), [this, s = self(), snapshot](Result<void> r) {
      if (!r && is_system_error_code(r.error()))
        return system_error(r.error());
    });
  } else {
    write_data(to_data(request), [this, s = self()](Result<void> r) {
      if (!r)
        shutdown(r.error());
    });
  }
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

    ASSERT(node_id == peer_id());

    if (r.value().info.log) {
      LOG_INFO << log_prefix() << r.value();
    }

    // round_trip_time = epoch_msec() - r.value().info.epoch;

    std::visit(
        [this, request_term, current_term](auto&& r) {
          if (auto s = server_.lock(); s)
            if (auto lk = s->usage_lock(); lk)
              s->handle_response(
                  self<RaftPeerConnection>(), request_term, current_term, std::move(r));
        },
        r.value().response);
  }
}

void RaftPeerConnection::read_response() {
  auto handler = [this, s = self()](Result<Data> msg) mutable { response_handler(std::move(msg)); };
  read_data(std::move(handler));
}

