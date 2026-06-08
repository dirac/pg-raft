#ifndef LIB_SERVERS_ZOOKEEPER_ZK_STATE_HPP_
#define LIB_SERVERS_ZOOKEEPER_ZK_STATE_HPP_

#include "lib/base/base.hpp"
#include "lib/servers/zookeeper/zk_types.hpp"

namespace banyandb::servers::zookeeper {

class ZkState {
 public:
  using PathId      = int32_t;
  using Path        = vector<PathId>;
  using PathSegment = boost::container::static_vector<PathId, 3>;

  ZkState(Zxid curr_zxid = Zxid{0}, std::time_t timestamp = 0, uint64_t counter = 0);

  ZkState(ZkState const& that);

  ZkState(ZkState&&);
  ZkState& operator=(ZkState&&);

  void swap(ZkState& that);

  // Creates a compact copy of self.
  ZkState copy() const;

  /// The request is a valid.
  static bool valid(ZkRequest const& request);

  static bool needs_quorum(ZkRequest const& result);

  /// Process the non-quorum request from a client.
  ZkResponse exec(ZkRequest request) const;

  /*
   * Process committed request.
   *  @param timestamp is the time that the leader got the request in as per leader's clock.
   *  @param log_idx   is the raft log index.
   *  @param request   the client request.
   */
  ZkResponse exec(std::time_t timestamp, RaftIndex log_idx, ZkRequest request);

  Zxid curr_zxid() const {
    auto lk = shared_lock();
    return curr_zxid_;
  }

  RaftIndex log_idx() const {
    auto lk = shared_lock();
    return curr_zxid_;
  }

  /*
   * Update time
   *  @param timestamp is the time that the leader created the ping request as per leader's clock.
   *  @param log_idx   is the raft log index.
   */
  void set_timestamp(std::time_t timestamp, RaftIndex log_idx);

  std::time_t timestamp() const {
    auto lk = shared_lock();
    return timestamp_;
  }

  uint64_t counter() const {
    auto lk = shared_lock();
    return counter_;
  }

  /// Periodically the leader finds the expired sessions and starts add it to the log.
  vector<Command> expire_sessions() const;

  // Compare two ZkState, a very expensive operation. Use it for testing only.
  bool operator==(ZkState const& that) const;

  struct NodeData {
    Stat   stat;
    string value;

    uint64_t data;

    static constexpr uint64_t kSequenceMask  = std::numeric_limits<uint64_t>::max() >> 4;
    static constexpr uint64_t kSequentialBit = 0x1ULL << 63U;
    static constexpr uint64_t kEphemeralBit  = 0x1ULL << 62U;
    static constexpr uint64_t kFrozenBit     = 0x1ULL << 61U;

    uint64_t sequence() const { return data & kSequenceMask; }
    void     inc_sequence() { ++data; }
    void     dec_sequence() { --data; }

    bool sequential() const { return (data & kSequentialBit) ? true : false; }
    bool ephemeral() const { return (data & kEphemeralBit) ? true : false; }

    void set_frozen(bool b) {
      if (b)
        data |= kFrozenBit;
      else
        data &= ~kFrozenBit;
    }
    bool frozen() const { return (data & kFrozenBit) ? true : false; }

    static uint64_t make_data(bool sequential, bool ephemeral) {
      uint64_t data = 0;
      if (sequential)
        data |= kSequentialBit;
      if (ephemeral)
        data |= kEphemeralBit;
      return data;
    }

    bool operator==(NodeData const&) const = default;

    template <class Archive>
    void serialize(Archive& ar, unsigned int const /*version*/) {
      ar & data;
      ar & stat;
      ar & value;
    }
  };

  template <class PathType>
  struct Session {
    uint64_t session_id;

    int32_t     timeout_seconds;
    std::time_t last_ping_time;

    int64_t request_no;  // The last request number.

    ResultStatus status;

    Zxid curr_zxid;  // The zxid returned during the last transaction.

    set<WatchEvent>       events;   //< The events returned during the last transaction.
    vector<CommandResult> results;  //< The result returnef during the last transaction.

    set<PathType> deletes;  // results in watch events when the session pings.

    set<PathType>                                   ephemeral;
    map<std::tuple<WatchEventType, PathType>, Zxid> watches;

    bool operator==(Session<PathType> const&) const = default;

    template <class Archive>
    void serialize(Archive& ar, unsigned int const /*version*/) {
      ar & session_id;

      ar & timeout_seconds;
      ar & last_ping_time;

      ar & request_no;
      ar & curr_zxid;

      ar & events;
      ar & results;

      ar & deletes;

      ar & ephemeral;
      ar & watches;
    }

    template <typename NewPathType>
    Session<NewPathType> copy(std::function<NewPathType(PathType const&)> const& new_path) const {
      Session<NewPathType> d;

      d.session_id      = session_id;
      d.timeout_seconds = timeout_seconds;
      d.last_ping_time  = last_ping_time;
      d.request_no      = request_no;
      d.curr_zxid       = curr_zxid;
      d.events          = events;
      d.results         = results;

      d.deletes = std::accumulate(deletes.begin(),
                                  deletes.end(),
                                  set<NewPathType>{},
                                  [&](set<NewPathType> result, PathType const& p) {
                                    result.emplace(new_path(p));
                                    return result;
                                  });

      d.ephemeral = std::accumulate(ephemeral.begin(),
                                    ephemeral.end(),
                                    set<NewPathType>{},
                                    [&](set<NewPathType> result, PathType const& p) {
                                      result.emplace(new_path(p));
                                      return result;
                                    });

      d.watches = std::accumulate(
          watches.begin(),
          watches.end(),
          map<std::tuple<WatchEventType, NewPathType>, Zxid>{},
          [&](map<std::tuple<WatchEventType, NewPathType>, Zxid>                 result,
              std::pair<std::tuple<WatchEventType, PathType> const, Zxid> const& elem) {
            auto type = std::get<WatchEventType>(elem.first);
            auto path = new_path(std::get<PathType>(elem.first));
            auto zxid = elem.second;

            result.emplace(std::make_tuple(type, std::move(path)), zxid);
            return result;
          });

      return d;
    }
  };

  template <typename K, typename V>
  void for_each(std::function<void(K const&, V const&)> func) const;

  // Create path id.
  bool add_string(PathId id, string_view str) {
    auto p = str_id(str);
    return id == p;
  }
  size_t num_strings() const { return path_seg_.size(); }
  void   for_each_string(std::function<void(PathId, string_view)> func) const {
    for (PathId id = 0; id < i4(path_seg_.size()); ++id)
      func(id, path_seg_.at(u8(id)));
  }
  template <>
  void for_each<PathId, string_view>(
      std::function<void(PathId const&, string_view const&)> func) const {
    return for_each_string(std::move(func));
  }

  bool add_node(Path path, NodeData data) {
    return data_.emplace(std::move(path), std::move(data)).second;
  }
  size_t num_nodes() const { return data_.size(); }
  void   for_each_node(std::function<void(Path const&, NodeData const&)> func) const {
    for (auto const& [path, data] : data_)
      func(path, data);
  }
  template <>
  void for_each<Path, NodeData>(std::function<void(Path const&, NodeData const&)> func) const {
    return for_each_node(std::move(func));
  }

  bool add_ttl_node(Path path, std::time_t time) {
    return ttl_nodes_.emplace(std::make_tuple(time, std::move(path))).second;
  }
  size_t num_ttl_nodes() const { return ttl_nodes_.size(); }
  void   for_each_ttl_node(std::function<void(Path const&, std::time_t const&)> func) const {
    for (std::tuple<std::time_t, Path> const& ttl_node : ttl_nodes_)
      func(std::get<1>(ttl_node), std::get<0>(ttl_node));
  }
  template <>
  void for_each<Path, std::time_t>(
      std::function<void(Path const&, std::time_t const&)> func) const {
    return for_each_ttl_node(std::move(func));
  }

  bool add_session(uint64_t session_id, Session<Path> session) {
    return sessions_.emplace(session_id, std::move(session)).second;
  }
  size_t num_sessions() const { return sessions_.size(); }
  void   for_each_session(std::function<void(uint64_t, Session<Path> const&)> func) const {
    for (auto const& [session_id, session] : sessions_)
      func(session_id, session);
  }
  template <>
  void for_each<uint64_t, Session<Path>>(
      std::function<void(uint64_t const&, Session<Path> const&)> func) const {
    return for_each_session(std::move(func));
  }

  /// Construct the node name from the path.
  string to_string(Path const& path) const;

  /// Active sessions.
  map<uint64_t, Session<string>> sessions() const;

  /// The tree data.
  map<string, NodeData> data() const;

  /// TTL nodes.
  set<std::tuple<std::time_t, string>> ttl_nodes() const;

  /// All the strings
  set<string> strings() const;

  /// Verify rollback was complete.
  bool verify_rollback(uint64_t session_id, ZkState const& before) const;

  static bool is_valid_value(string const& value) {
    return std::accumulate(std::begin(value), std::end(value), true, [](bool result, char ch) {
      return result && (ch >= 32 && ch <= 126);
    });
  }

  static bool is_valid_path(string const& path, bool seq = false) {
    if (!path.empty() && (path.front() != '/' || path.back() == '/'))
      return false;

    size_t segment = 0;
    auto   iter    = std::begin(path);
    ++iter;

    while (iter != std::end(path)) {
      if (*iter == '/') {
        if (segment > kSegmentSizeLimit)
          return false;
        segment = 0;
      } else if (*iter < 32 || *iter > 126 || *iter == '\\')
        return false;
      else
        ++segment;

      ++iter;
    }
    return seq ? segment <= (kSegmentSizeLimit - kSequenceDigits) : segment <= kSegmentSizeLimit;
  }

 private:
  void init_string_buffers() {
    path_buff_.emplace_front();
    std::memset(path_buff_.front().data(), 0, path_buff_.front().size());
    path_buff_pos_ = 0;
  }

  // Expand the encode paths to string representation.
  set<string> expand_paths(set<Path> const& paths) const;

  // Expand the encode paths to string representation.
  map<std::tuple<WatchEventType, string>, Zxid> expand_paths(
      map<std::tuple<WatchEventType, Path>, Zxid> const& watches) const;

  using UndoCommand = std::function<void(Session<Path>*)>;
  static void noop(Session<Path>*) {}

 private:
  template <bool seq>
  static std::tuple<PathId, PathId> split(uint64_t const n) {
    static_assert((kSequenceThreshold << 1) < (0x1ULL << 62));

    assert(n > 0);
    assert(n < kSequenceThreshold);

    auto num = seq ? n + kSequenceThreshold : n;

    int32_t hi =
        static_cast<int32_t>(static_cast<uint32_t>(num >> 31) | static_cast<uint32_t>(0x80000000U));

    int32_t lo = static_cast<int32_t>(static_cast<uint32_t>(num & 0x3FFFFFFFULL) |
                                      static_cast<uint32_t>(0x80000000ULL));

    assert(hi < 0);
    assert(lo < 0);

    assert(num == ((static_cast<uint64_t>(hi) & 0x3FFFFFFFULL) << 31 |
                   (static_cast<uint64_t>(lo) & 0x3FFFFFFFULL)));

    auto r = std::make_tuple(hi, lo);

    assert(std::get<bool>(merge(r)) == seq);
    assert(std::get<uint64_t>(merge(r)) == n);

    return r;
  }

  static std::tuple<uint64_t, bool> merge(PathId hi, PathId lo) {
    assert(hi < 0);
    assert(lo < 0);

    auto num = ((static_cast<uint64_t>(hi) & 0x3FFFFFFFULL) << 31 |
                (static_cast<uint64_t>(lo) & 0x3FFFFFFFULL));

    return std::make_tuple(num > kSequenceThreshold ? num - kSequenceThreshold : num,
                           num > kSequenceThreshold);
  }
  static std::tuple<uint64_t, bool> merge(std::tuple<PathId, PathId> t) {
    return merge(std::get<0>(t), std::get<1>(t));
  }

  PathId str_id(string_view str);

  static std::tuple<string, string> parent_and_node(string const& path);

  std::optional<PathSegment> try_num(string_view str) const;
  std::optional<PathSegment> try_seq(string_view str) const;
  std::optional<PathSegment> try_str(string_view str) const;

  static PathSegment pop(Path& path);
  static void        push(Path& path, PathSegment const& seg);

  // using Iter = decltype(Path{}.begin());

  template <typename Iter>
  static PathSegment segment(Iter& iter, Iter end);

  /// Get the path segment
  string to_string(PathSegment const& seg) const;

  /// Extend the path with the new segment. Adds only if the path exists in the tree.
  bool append(Path& path, string_view str) const;

  /// Given the path, a vector of path ids for each segment from the name.
  std::optional<Path> to_path(string_view path) const;

  static bool new_session(ZkRequest const& request);
  static bool close_session(ZkRequest const& request);

  bool notify(std::tuple<WatchEventType, Path> const& w, Zxid zxid) const;

  /// Create events to be reported on watch trigger.
  /// Deletes the watches triggered from the map.
  set<WatchEvent> trigger_watches(map<std::tuple<WatchEventType, Path>, Zxid>& watches,
                                  set<Path>&                                   deletes) const;

  ZkResponse zk_response(Session<Path> const* session) const;

  vector<Zxid> update_zxid(Path path, unsigned depth);

  template <bool parent>
  void update(Path& path, vector<Zxid>& old_values, unsigned depth);

  template <bool parent>
  void rollback(Path& path, vector<Zxid>& old_values);

  void rollback_zxid(Path path, vector<Zxid> old_values);

  ZkResponse exec(Session<Path>* session, ZkRequest request);
  ZkResponse local_exec(Session<Path>* session, ZkRequest request) const;

  void erase_expired_nodes();

  CommandResult exec(GetValueCommand const& cmd) const;
  CommandResult exec(NodeExistsCommand const& cmd) const;
  CommandResult exec(GetChildrenCommand const& cmd) const;

  // Return command result and the undo function.
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, NewSessionCommand cmd);
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, CloseSessionCommand cmd);
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, GetValueCommand cmd);
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, GetChildrenCommand cmd);
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, NodeExistsCommand cmd);
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, SetValueCommand cmd);
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, CreateNodeCommand cmd);
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, EraseNodeCommand cmd);
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, ExtendTTLCommand cmd);
  std::tuple<CommandResult, UndoCommand> exec(Session<Path>* session, IncCounterCommand cmd);

  std::tuple<CommandResult, UndoCommand> exec_with_watch(Session<Path>*           session,
                                                         NodeExistsCommand const& cmd);
  std::tuple<CommandResult, UndoCommand> exec_without_watch(NodeExistsCommand const& cmd);

  Session<Path>* new_session(uint64_t                 session_id,
                             int64_t                  request_no,
                             Zxid                     curr_zxid,
                             NewSessionCommand const& cmd);

  Session<Path>*       get_session(uint64_t session_id);
  Session<Path> const* get_session(uint64_t session_id) const;

  static bool is_subtree(Path const& parent, Path const& child);

  string_view internal_string_view(string_view str);

  struct Tree {
    using iterator       = map<Path, NodeData>::iterator;
    using const_iterator = map<Path, NodeData>::const_iterator;

    iterator       begin() { return tree_.begin(); }
    const_iterator begin() const { return tree_.begin(); }

    iterator       end() { return tree_.end(); }
    const_iterator end() const { return tree_.end(); }

    std::pair<iterator, bool> emplace(Path key, NodeData data) {
      if (auto pos = lookup_.find(&key); pos != lookup_.end())
        return std::make_pair(pos->second, false);

      auto [iter, inserted] = tree_.emplace(std::move(key), std::move(data));
      assert(inserted);
      lookup_.emplace(&(iter->first), iter);

      return std::make_pair(iter, true);
    }

    iterator find(Path const& key) {
      if (auto pos = lookup_.find(&key); pos != lookup_.end())
        return pos->second;
      return tree_.end();
    }
    const_iterator find(Path const& key) const {
      if (auto pos = lookup_.find(&key); pos != lookup_.end())
        return pos->second;
      return tree_.end();
    }

    size_t erase(Path const& key) {
      if (auto pos = lookup_.find(&key); pos != lookup_.end()) {
        tree_.erase(pos->second);
        lookup_.erase(pos);

        return 1U;
      }
      return 0U;
    }

    void clear() {
      lookup_.clear();
      tree_.clear();
    }

    bool empty() const { return tree_.empty(); }

    size_t size() const { return tree_.size(); }

    bool operator==(Tree const& that) const { return tree_ == that.tree_; }

    Tree() {}

    Tree(Tree const& that) : tree_(that.tree_) {
      for (auto iter = tree_.begin(); iter != tree_.end(); ++iter)
        lookup_.emplace(&(iter->first), iter);
    }

    Tree(Tree&&)            = default;
    Tree& operator=(Tree&&) = default;

   private:
    struct Hash {
      std::size_t operator()(Path const* vec) const {
        std::size_t seed = vec->size();
        for (PathId i : *vec) {
          auto x = static_cast<uint32_t>(i);

          x = ((x >> 16) ^ x) * 0x45d9f3b;
          x = ((x >> 16) ^ x) * 0x45d9f3b;
          x = (x >> 16) ^ x;
          seed ^= x + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
      }
    };
    struct Equals {
      bool operator()(Path const* a, Path const* b) const { return *a == *b; }
    };

    map<Path, NodeData>                                     tree_;
    unordered_flat_map<Path const*, iterator, Hash, Equals> lookup_;
  };

  // All paths are broken into segments delimited by '/'. Each segment of the path is given an id.
  unordered_map<string_view, PathId> path_ids_;
  vector<string_view>                path_seg_;

  // The segments are represented as string_view and points to a segment in path_buff_.
  list<array<char, kBufferSize>> path_buff_;
  uint64_t                       path_buff_pos_;

  // The tree.
  Tree data_;

  // TTL nodes
  set<std::tuple<std::time_t, Path>, std::greater<std::tuple<std::time_t, Path>>> ttl_nodes_;

  // Active sessions
  unordered_map<uint64_t, Session<Path>> sessions_;

  // Counter
  uint64_t counter_{0};

  Zxid        curr_zxid_;
  std::time_t timestamp_;

  mutable std::atomic<unsigned> last_session_expiry_check_{0};

  friend class boost::serialization::access;
  friend std::ostream& operator<<(std::ostream& out, ZkState const& state);

  std::shared_lock<std::shared_mutex> shared_lock() const {
    return std::shared_lock<std::shared_mutex>{mtx_};
  }
  std::unique_lock<std::shared_mutex> unique_lock() {
    return std::unique_lock<std::shared_mutex>{mtx_};
  }

  // For global state.
  mutable std::shared_mutex mtx_;

  // Session should be locked for read/write fn.
  mutable std::array<std::mutex, 0x1 << 12> session_mutexes_;

  auto& session_mutex(uint64_t session_id) const {
    return session_mutexes_.at(session_id & ((0x1 << 12) - 1U));
  }

  template <typename PathType>
  static bool has_session_lock(Session<PathType> const* locked_session,
                               Session<PathType> const* session) {
    auto locked_session_id = locked_session ? locked_session->session_id : 0ULL;
    auto session_id        = session ? session->session_id : 0ULL;
    return (locked_session_id & ((0x1 << 12) - 1U)) == (session_id & ((0x1 << 12) - 1U));
  }

  std::unique_lock<std::mutex> session_lock(uint64_t session_id) const {
    return std::unique_lock<std::mutex>(session_mutex(session_id));
  }

  template <typename PathType>
  std::tuple<Session<PathType>*, std::unique_lock<std::mutex>> session_lock(
      Session<PathType> const* s) const {
    return {const_cast<Session<PathType>*>(s), session_lock(s ? s->session_id : 0ULL)};
  }

 public:
  constexpr static size_t  kSegmentSizeLimit  = 64U;
  constexpr static size_t  kSequenceDigits    = 19U;
  constexpr static int64_t kSequenceThreshold = 1'000'000'000'000'000'000ULL;

#ifndef NDEBUG
  constexpr static unsigned kSessionExpiryCheck = 2000;  // How often to check for session expiry.
#else
  constexpr static unsigned kSessionExpiryCheck = 0;  // How often to check for session expiry.
#endif
};

std::ostream& operator<<(std::ostream& out, ZkState const& state);

}  // namespace banyandb::servers::zookeeper

#endif
