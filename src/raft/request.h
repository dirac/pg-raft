

struct StatusRequest {};

struct StatusResponse {
  std::optional<LogSeqNumber> backup_lsn;
  std::optional<LogSeqNumber> flushed_lsn;
  std::optional<LogSeqNumber> current_lsn;
};

struct SnapshotRequest {};

struct SnapshotResponse {};

struct Request {
  std::variant<StatusRequest, SnapshotRequest> req;
};

struct Response {
  bool                                           success;
  std::variant<StatusResponse, SnapshotResponse> res;
};
