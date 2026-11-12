#pragma once

#include "src/base/base.h"

// The protocol of a client of a raft server (RaftServerConnection <-> RaftClientConnection):
//
//  - The proxy, to learn which server is the leader (from the heartbeat status byte) and its
//    status, and to take a backup of PostgreSQL.
//  - A raft server whose log was replaced by an InstallSnapshot (RaftState::needs_backup), to fetch
//    a PostgreSQL backup from another server: from a standby (the instance is stopped and its data
//    directory copied) or, failing that, from the primary (pg_basebackup).

/// The status of the server and of its PostgreSQL instance.
struct StatusRequest {
  template <class Archive>
  void serialize(Archive& /* ar */, unsigned int const /*version*/) {}
};

struct StatusResponse {
  bool leader;

  RaftIndex applied_idx;

  std::optional<uint64_t> primary_flushed_lsn;
  std::optional<uint64_t> standby_applied_lsn;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & leader;
    ar & applied_idx;
    ar & primary_flushed_lsn;
    ar & standby_applied_lsn;
  }
};

/// A backup of the PostgreSQL instance of the server. The server answers with a SnapshotResponse,
/// followed (on success) by the files, in the order of SnapshotResponse::files (read_file()).
struct SnapshotRequest {
  /// The requester has applied up to this index; a server that is behind it refuses.
  RaftIndex applied_idx;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & applied_idx;
  }
};

struct SnapshotResponse {
  bool success;

  /** files that follow this response, relative to the data directory. */
  vector<string> files;

  /** Symbolic links: relative path -> target. */
  map<string, string> links;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & success;
    ar & files;
    ar & links;
  }
};

using ClientRequest = std::variant<SnapshotRequest, StatusRequest>;
