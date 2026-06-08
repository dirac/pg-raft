#pragma once

#include "src/base/base.h"

enum class ResultStatus : uint8_t {
  kOk               = 0,  /// Success
  kInvalidLeader    = 1,  /// The current server is not the leader.
  kNoNode           = 2,  /// Node doesn't exists
  kBadVersion       = 3,  /// Version mismatch
  kNodeExists       = 4,
  kSessionExpired   = 5,
  kStaleRequest     = 6,
  kNodeFrozen       = 7,
  kNodeSequential   = 8,
  kNodeEphemeral    = 9,
  kHasChildren      = 10,
  kInvalidNodePath  = 11,
  kInvalidNodeValue = 12,
  kSequenceOverflow = 13,  /// Sequence overflows at 10^18 (i.e., > 2^59).
  kNoTTLNode        = 14,
  kInvalidExpiry    = 15,
  kUnknown          = 16,

  kProgramBug       = 254,
  kInvalidArguments = 255,
};

inline char const* to_string(ResultStatus s) {
#define TO_STR(x)       \
  case ResultStatus::x: \
    return #x
  switch (s) {
    TO_STR(kOk);
    TO_STR(kInvalidLeader);
    TO_STR(kNoNode);
    TO_STR(kBadVersion);
    TO_STR(kNodeExists);
    TO_STR(kSessionExpired);
    TO_STR(kStaleRequest);
    TO_STR(kNodeFrozen);
    TO_STR(kNodeSequential);
    TO_STR(kNodeEphemeral);
    TO_STR(kHasChildren);
    TO_STR(kInvalidNodePath);
    TO_STR(kInvalidNodeValue);
    TO_STR(kSequenceOverflow);
    TO_STR(kNoTTLNode);
    TO_STR(kInvalidExpiry);
    TO_STR(kUnknown);

    TO_STR(kProgramBug);
    TO_STR(kInvalidArguments);
  }
#undef TO_STR
  return "";
}

enum class WatchEventType : uint8_t {
  kNodeEvent  = 1,  /// The value of a node changed.
  kChildEvent = 2,  /// A child is created or deleted.
};

inline std::array<WatchEventType, 2> watch_event_types() {
  return {WatchEventType::kNodeEvent, WatchEventType::kChildEvent};
}

enum class SessionState : uint8_t {
  kConnected = 1,
  kExpired   = 2,
  kMoved     = 3,
  kUnknown   = 4,
};

struct Stat {
  Zxid mzxid;  // modified log_idx for the node.
  Zxid czxid;  // the modified log_idx for the subtree.

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & mzxid;
    ar & czxid;
  }

  bool operator==(Stat const&) const = default;
};
inline std::ostream& operator<<(std::ostream& out, Stat const& s) {
  return out << "mzxid = " << INT(s.mzxid) << " czxid = " << INT(s.czxid);
}

// Create a new session.
struct NewSessionCommand {
  string  name;
  int32_t timeout_seconds;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & name;
    ar & timeout_seconds;
  }

  bool operator==(NewSessionCommand const&) const = default;
};

struct NewSessionResult {
  uint64_t session_id;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & session_id;
  }

  bool operator==(NewSessionResult const&) const = default;
};

struct CloseSessionCommand {
  uint64_t session_id;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & session_id;
  }

  bool operator==(CloseSessionCommand const&) const = default;
};

struct CloseSessionResult {
  template <class Archive>
  void serialize(Archive& /*ar*/, unsigned int const /*version*/) {}

  bool operator==(CloseSessionResult const&) const = default;
};

struct IncCounterCommand {
  uint64_t increment;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & increment;
  }

  bool operator==(IncCounterCommand const&) const = default;
};

struct IncCounterResult {
  uint64_t value;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & value;
  }
  bool operator==(IncCounterResult const&) const = default;
};

// Status = Ok, NoNode
struct GetValueCommand {
  string path;
  bool   watch;
  bool   quorum_read{false};

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & watch;
    ar & quorum_read;
  }

  bool operator==(GetValueCommand const&) const = default;
};

struct GetValueResult {
  string value;
  Stat   stat;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & value;
    ar & stat;
  }
  bool operator==(GetValueResult const&) const = default;
};

// Status = Ok, NoDirectory
struct GetChildrenCommand {
  string path;
  bool   watch;
  bool   quorum_read{false};

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & watch;
    ar & quorum_read;
  }

  bool operator==(GetChildrenCommand const&) const = default;
};

struct GetChildrenResult {
  string         path;
  vector<string> children;
  Stat           stat;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & children;
    ar & stat;
  }
  bool operator==(GetChildrenResult const&) const = default;
};

// ResultStatus = Ok, NoNode, BadVersion
struct NodeExistsCommand {
  string              path;
  std::optional<Zxid> zxid;
  bool                quorum_read{false};

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & zxid;
    ar & quorum_read;
  }
  bool operator==(NodeExistsCommand const&) const = default;
};

struct NodeExistsResult {
  Stat stat;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & stat;
  }
  bool operator==(NodeExistsResult const&) const = default;
};

struct SetValueCommand {
  string              path;
  string              value;
  std::optional<Zxid> zxid;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & value;
    ar & zxid;
  }
  bool operator==(SetValueCommand const&) const = default;
};

struct SetValueResult {
  Stat stat;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & stat;
  }
  bool operator==(SetValueResult const&) const = default;
};

struct CreateNodeCommand {
  string path;
  string value;
  bool   ephemeral{false};
  bool   sequential{false};
  bool   if_not_exists{false};

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & value;
    ar & ephemeral;
    ar & sequential;
    ar & if_not_exists;
  }
  bool operator==(CreateNodeCommand const&) const = default;
};

struct CreateNodeResult {
  string path;
  Stat   stat;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & stat;
  }
  bool operator==(CreateNodeResult const&) const = default;
};

struct EraseNodeCommand {
  string                 path;
  std::optional<Zxid>    zxid;
  std::optional<int16_t> ttl_secs;  // Erase the node in ttl_secs, overrides ephemeral property.

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & zxid;
    ar & ttl_secs;
  }
  bool operator==(EraseNodeCommand const&) const = default;
};

struct EraseNodeResult {
  std::optional<std::time_t>
      expiry;  // The time the node will be removed from the tree if ttl was requested.

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & expiry;
  }
  bool operator==(EraseNodeResult const&) const = default;
};

struct ExtendTTLCommand {
  string      path;
  std::time_t expiry;
  int16_t     ttl_secs;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & expiry;
    ar & ttl_secs;
  }
  bool operator==(ExtendTTLCommand const&) const = default;
};

struct ExtendTTLResult {
  std::time_t expiry;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & expiry;
  }
  bool operator==(ExtendTTLResult const&) const = default;
};

using Command = std::variant<NewSessionCommand,
                             CloseSessionCommand,
                             GetValueCommand,
                             GetChildrenCommand,
                             NodeExistsCommand,
                             SetValueCommand,
                             CreateNodeCommand,
                             EraseNodeCommand,
                             ExtendTTLCommand,
                             IncCounterCommand>;

struct CommandResult {
  ResultStatus status;

  std::variant<std::monostate,
               NewSessionResult,
               CloseSessionResult,
               GetValueResult,
               GetChildrenResult,
               NodeExistsResult,
               SetValueResult,
               CreateNodeResult,
               EraseNodeResult,
               ExtendTTLResult,
               IncCounterResult>
      result;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & status;
    ar & result;
  }
  bool operator==(CommandResult const&) const = default;
};

struct ZkRequest {
  uint64_t session_id;
  int64_t  request_no;  // An increasing number for every request by the session.

  vector<Command> commands;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & session_id;
    ar & request_no;
    ar & commands;
  }
  bool operator==(ZkRequest const&) const = default;
};
inline std::ostream& operator<<(std::ostream& out, ZkRequest const& r) {
  return out << r.session_id << ':' << r.request_no;
}

struct WatchEvent {
  string         path;
  WatchEventType type;

  bool operator==(WatchEvent const&) const = default;

  bool operator<(WatchEvent const& that) const {
    return path < that.path || (path == that.path && type < that.type);
  }

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & path;
    ar & type;
  }
};

inline std::ostream& operator<<(std::ostream& out, WatchEvent const& we) {
  return out << we.path << " " << (we.type == WatchEventType::kNodeEvent ? "node" : "child");
}

struct ZkResponse {
  ResultStatus status;
  SessionState state;
  Zxid         zxid;

  set<WatchEvent>       events;
  vector<CommandResult> results;

  bool operator==(ZkResponse const&) const = default;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & status;
    ar & state;
    ar & zxid;
    ar & events;
    ar & results;
  }
};

