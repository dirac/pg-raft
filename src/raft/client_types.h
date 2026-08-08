#pragma once

struct StatusRequest {};

struct StatusResponse {
  RaftIndex applied_idx;

  std::optional<uint64_t> primary_flushed_lsn;
  std::optional<uint64_t> standby_applied_lsn;
};

struct SnapshotRequest {
  RaftIndex applied_idx;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & applied_idx;
  }
};

struct SnapshotResponse {
  bool success;

  /** files that follow this response. */
  vector<string> files;

  /** Links */
  map<string, string> links;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & success;
    ar & files;
    ar & links;
  }
};

using ClientRequest = std::variant<SnapshotRequest, StatusRequest>;

