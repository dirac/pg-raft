#include "src/raft/zk_state.h"

ZkState::ZkState(Zxid curr_zxid, std::time_t timestamp, uint64_t counter)
    : counter_(counter), curr_zxid_(curr_zxid), timestamp_(timestamp) {
  init_string_buffers();
}

ZkState::ZkState(ZkState const& that)
    : data_(that.data_),
      ttl_nodes_(that.ttl_nodes_),
      sessions_(that.sessions_),
      counter_(that.counter_),
      curr_zxid_(that.curr_zxid_),
      timestamp_(that.timestamp_) {
  init_string_buffers();

  for (size_t idx = 0; idx < that.path_seg_.size(); ++idx) {
    auto n_idx = str_id(that.path_seg_.at(idx));
    (void)n_idx;
    assert(i8(idx) == i8(n_idx));
  }
}

ZkState::ZkState(ZkState&& that) {
  init_string_buffers();
  swap(that);
}

ZkState& ZkState::operator=(ZkState&& that) {
  swap(that);

  return *this;
}

void ZkState::swap(ZkState& that) {
  auto that_lk = that.unique_lock();
  auto this_lk = unique_lock();

#define SWAP(v) std::swap(v, that.v)

  SWAP(path_ids_);
  SWAP(path_seg_);
  SWAP(path_buff_);
  SWAP(path_buff_pos_);

  SWAP(data_);
  SWAP(ttl_nodes_);
  SWAP(sessions_);
  SWAP(counter_);
  SWAP(curr_zxid_);
  SWAP(timestamp_);

#undef SWAP
}

ZkState ZkState::copy() const {
  auto lk = shared_lock();

  auto sorted_strings = [this]() -> vector<std::tuple<string_view, PathId>> {
    unordered_flat_set<PathId> path_ids;

    for (auto const& [path, _] : data_)
      std::for_each(path.begin(), path.end(), [&path_ids](PathId id) {
        if (id >= 0)
          path_ids.emplace(id);
      });

    for (auto const& [_, session] : sessions_)
      for (Path const& path : session.deletes)
        std::for_each(path.begin(), path.end(), [&path_ids](PathId id) {
          if (id >= 0)
            path_ids.emplace(id);
        });

    vector<std::tuple<string_view, PathId>> result;
    result.reserve(path_ids.size());

    result = std::accumulate(
        path_ids.begin(), path_ids.end(), std::move(result), [this](auto result, PathId id) {
          result.emplace_back(path_seg_.at(u8(id)), id);
          return result;
        });

    std::sort(result.begin(), result.end());
    return result;
  };

  ZkState state;

  state.counter_   = counter_;
  state.curr_zxid_ = curr_zxid_;
  state.timestamp_ = timestamp_;

  unordered_flat_map<PathId, PathId> new_mapping;
  new_mapping.reserve(path_seg_.size() * 2U);

  for (auto const& [s, id] : sorted_strings())
    new_mapping.emplace(id, state.str_id(s));

  auto new_path = [this, &state, &new_mapping](Path const& path) -> Path {
    Path result;
    result.reserve(path.size());

    for (PathId id : path) {
      assert(id < 0 || new_mapping.find(id) != new_mapping.end());
      result.emplace_back(id < 0 ? id : new_mapping.at(id));
    }

    // Used only in debug builds (asserts).
    (void)this;
    (void)state;

    assert(path.size() == result.size());
    assert(to_string(path) == state.to_string(result));

    return result;
  };

  for (auto const& [path, data] : data_)
    state.data_.emplace(new_path(path), data);

  for (auto const& [time, path] : ttl_nodes_)
    state.ttl_nodes_.emplace(time, new_path(path));

  for (auto const& [session_id, session] : sessions_)
    state.sessions_.emplace(session_id, session.copy<Path>(new_path));

  return state;
}

string_view ZkState::internal_string_view(string_view str) {
  if ((str.size() + 1U) < (path_buff_.front().size() - path_buff_pos_)) {
    path_buff_.emplace_front();
    std::memset(path_buff_.front().data(), 0, path_buff_.front().size());

    path_buff_pos_ = 0;
  }

  char* p = path_buff_.front().data() + path_buff_pos_;
  path_buff_pos_ += str.size() + 1U;

  std::memcpy(p, str.data(), str.size());
  return string_view{p, str.size()};
}

ZkState::PathId ZkState::str_id(string_view str) {
  if (auto pos = path_ids_.find(str); pos != path_ids_.end())
    return pos->second;

  path_seg_.emplace_back(internal_string_view(str));
  path_ids_.emplace(path_seg_.back(), path_seg_.size() - 1U);

  return PathId{i4(path_seg_.size()) - 1};
}

std::optional<ZkState::PathSegment> ZkState::try_str(string_view str) const {
  if (auto pos = path_ids_.find(str); pos != path_ids_.end())
    return PathSegment{pos->second};
  return std::nullopt;
}

std::optional<ZkState::PathSegment> ZkState::try_num(string_view str) const {
  uint64_t r = 0;
  for (auto ch : str) {
    if (ch < '0' || ch > '9')
      return std::nullopt;
    r = r * 10U + (u8(ch) - u8('0'));

    // The segment should not start with a zero and should not be greater than the sequence
    // threshold. All numbers greater than sequence threshold are sequence numbers (with
    // kSequenceThreshold added)
    if (r == 0 || r >= kSequenceThreshold)
      return std::nullopt;
  }
  if (r == 0 || r >= kSequenceThreshold)
    return std::nullopt;

  auto [n1, n2] = split<false>(r);
  return PathSegment{n1, n2};
}

std::optional<ZkState::PathSegment> ZkState::try_seq(string_view seg) const {
  static_assert(kSequenceThreshold == 999999999999999999LL + 1LL);

  if (seg.size() <= kSequenceDigits)
    return std::nullopt;

  auto prefix = string_view{seg.data(), seg.size() - kSequenceDigits};
  auto number = string_view{seg.data() + (seg.size() - kSequenceDigits), kSequenceDigits};

  PathSegment result;
  if (auto seg = try_str(prefix); seg)
    result.push_back(seg->at(0));
  else
    return std::nullopt;

  if (number.at(0) != '0')
    return std::nullopt;

  uint64_t seq = 0;
  for (auto ch : number) {
    seq = seq * 10U + (u8(ch) - u8('0'));

    if (ch < '0' || ch > '9')
      return std::nullopt;
    if (seq >= kSequenceThreshold)
      return std::nullopt;
  }

  auto [n1, n2] = split<true>(seq);
  result.push_back(n1);
  result.push_back(n2);

  assert(to_string(result) == seg);
  return result;
}

string ZkState::to_string(PathSegment const& seg) const {
  switch (seg.size()) {
    case 1U:
      return string{path_seg_.at(u8(seg.at(0)))};

    case 2U: {
      auto [num, _] = merge(std::make_tuple(seg.at(0), seg.at(1)));
      return fn::format("{}", num);
    }

    case 3U: {
      auto [num, _] = merge(std::make_tuple(seg.at(1), seg.at(2)));
      return fn::format("{}{:019d}", path_seg_.at(u8(seg.at(0))), num);
    }

    default:
      ASSERT(false);
  }
  return string{};
}

template <typename Iter>
ZkState::PathSegment ZkState::segment(Iter& iter, Iter end) {
  assert(iter != end);

  PathId a = *iter;
  ++iter;

  if (a >= 0) {
    if (iter != end && *iter < 0) {
      assert((iter + 1) != end);

      auto n1 = *iter;
      auto n2 = *(iter + 1);

      auto [_, seq] = merge(std::make_tuple(n1, n2));
      if (!seq)
        return PathSegment{a};

      ++iter;
      assert(iter != end);

      ++iter;
      return PathSegment{a, n1, n2};

    } else
      return PathSegment{a};
  }
  assert(iter != end);

  auto n1 = a;
  auto n2 = *iter;

  ++iter;
  return PathSegment{n1, n2};
}

bool ZkState::append(Path& path, string_view str) const {
  if (auto seg = try_num(str); seg) {
    push(path, seg.value());

    if (data_.find(path) != data_.end())
      return true;

    pop(path);
  }

  if (auto seg = try_seq(str); seg) {
    push(path, seg.value());

    if (data_.find(path) != data_.end())
      return true;

    pop(path);
  }

  if (auto seg = try_str(str); seg) {
    push(path, seg.value());

    if (data_.find(path) != data_.end())
      return true;

    pop(path);
  }
  return false;
}

std::optional<ZkState::Path> ZkState::to_path(string_view path) const {
  if (path.empty())
    return Path{};

  Path result;
  result.reserve(u8(std::count(path.begin(), path.end(), '/')) + 1ULL);

  char const* beg = path.data();
  char const* end = path.data() + path.size();

  // Skip the first '/'
  ++beg;

  size_t sz = 0;
  do {
    if (beg + sz == end) {
      if (!append(result, string_view{beg, sz}))
        return std::nullopt;

      beg = beg + sz;
      sz  = 0;
    } else if (beg[sz] == '/') {
      if (!append(result, string_view{beg, sz}))
        return std::nullopt;

      beg = beg + sz + 1;
      sz  = 0;
    } else
      ++sz;
  } while (beg != end);

  assert(path == to_string(result));
  return result;
}

string ZkState::to_string(ZkState::Path const& path) const {
  string result;

  auto iter = path.begin();
  while (iter != path.end()) {
    result += '/';
    result += to_string(segment(iter, path.end()));
  }
  return result;
}

ZkState::PathSegment ZkState::pop(ZkState::Path& path) {
  auto last = path.back();
  path.pop_back();

  if (last >= 0)
    return PathSegment{last};

  auto n2 = last;
  auto n1 = path.back();
  path.pop_back();

  auto [_, seq] = merge(std::make_tuple(n1, n2));
  if (!seq)
    return PathSegment{n1, n2};

  auto s1 = path.back();
  path.pop_back();

  return PathSegment{s1, n1, n2};
}

void ZkState::push(ZkState::Path& path, ZkState::PathSegment const& seg) {
  for (auto p : seg)
    path.emplace_back(p);
}

bool ZkState::needs_quorum(ZkRequest const& request) {
  for (auto const& cmd : request.commands) {
    if (std::holds_alternative<GetValueCommand>(cmd)) {
      if (std::get<GetValueCommand>(cmd).quorum_read || std::get<GetValueCommand>(cmd).watch)
        return true;
      continue;
    }
    if (std::holds_alternative<NodeExistsCommand>(cmd)) {
      if (std::get<NodeExistsCommand>(cmd).quorum_read)
        return true;
      continue;
    }
    if (std::holds_alternative<GetChildrenCommand>(cmd)) {
      if (std::get<GetChildrenCommand>(cmd).quorum_read || std::get<GetChildrenCommand>(cmd).watch)
        return true;
      continue;
    }
    return true;
  }
  return false;
}

bool ZkState::new_session(ZkRequest const& request) {
  return request.commands.size() == 1 &&
         std::holds_alternative<NewSessionCommand>(request.commands.front());
}

bool ZkState::close_session(ZkRequest const& request) {
  return !request.commands.empty() &&
         std::accumulate(request.commands.begin(),
                         request.commands.end(),
                         true,
                         [](bool result, auto const& cmd) {
                           return result && std::holds_alternative<CloseSessionCommand>(cmd);
                         });
}

bool ZkState::valid(ZkRequest const& request) {
  if (new_session(request))
    return true;
  if (close_session(request))
    return true;

  for (Command const& cmd : request.commands)
    if (std::holds_alternative<NewSessionCommand>(cmd) ||
        std::holds_alternative<CloseSessionCommand>(cmd))
      return false;

  return true;
}

bool ZkState::notify(std::tuple<WatchEventType, ZkState::Path> const& watch, Zxid zxid) const {
  auto pos = data_.find(std::get<Path>(watch));
  assert(pos != data_.end());

  if (pos == data_.end()) {
    return true;
  } else {
    switch (std::get<WatchEventType>(watch)) {
      case WatchEventType::kNodeEvent:
        return (zxid < pos->second.stat.mzxid);
        break;

      case WatchEventType::kChildEvent:
        return (zxid < pos->second.stat.czxid);
    }
  }
}

set<WatchEvent> ZkState::trigger_watches(map<std::tuple<WatchEventType, Path>, Zxid>& watches,
                                         set<Path>& deletes) const {
  set<WatchEvent> events;

  for (auto iter = deletes.begin(); iter != deletes.end();) {
    Path const& d = *iter;
    for (WatchEventType type : watch_event_types()) {
      if (auto pos = watches.find(std::make_tuple(type, d)); pos != watches.end()) {
        events.emplace(WatchEvent{.type = type, .path = to_string(d)});
        watches.erase(pos);
      }
    }
    iter = deletes.erase(iter);
  }

  for (auto iter = watches.begin(); iter != watches.end();) {
    if (notify(iter->first, iter->second)) {
      events.emplace(to_string(std::get<Path>(iter->first)), std::get<WatchEventType>(iter->first));
      iter = watches.erase(iter);
    } else
      ++iter;
  }

  return events;
}

void ZkState::erase_expired_nodes() {
  auto iter = ttl_nodes_.begin();
  while (iter != ttl_nodes_.end() && std::get<std::time_t>(*iter) <= timestamp_) {
    Path const& path = std::get<Path>(*iter);

    // unfreeze so that it can be deleted.
    data_.find(path)->second.set_frozen(false);

    // Do the delete.
    auto [result, _] = exec(nullptr, EraseNodeCommand{.path = to_string(path)});
    assert(result.status == ResultStatus::kOk);

    // Erase from the ttl_nodes list.
    iter = ttl_nodes_.erase(iter);
  }
}

void ZkState::set_timestamp(std::time_t t, RaftIndex log_idx) {
  auto lk = unique_lock();

  assert(curr_zxid_ + 1 == log_idx);

  curr_zxid_ = Zxid{log_idx};
  timestamp_ = std::max(t, timestamp_);

  erase_expired_nodes();
}

// Ping is processed by leader, this is not processed by a non-leader.
ZkResponse ZkState::exec(ZkRequest request) const {
  assert(!needs_quorum(request));

  auto lk = shared_lock();

  Session<Path> const* s = get_session(request.session_id);
  if (!s)
    return ZkResponse{
        .state   = SessionState::kExpired,
        .status  = ResultStatus::kSessionExpired,
        .zxid    = curr_zxid_,
        .events  = {},
        .results = {},
    };

  auto [session, _]       = session_lock(s);
  session->last_ping_time = std::time(nullptr);

  return local_exec(session, std::move(request));
}

ZkResponse ZkState::exec(std::time_t time, RaftIndex log_idx, ZkRequest request) {
  assert(valid(request));

  auto lk = unique_lock();

  assert(curr_zxid_ + 1 == log_idx);

  curr_zxid_ = Zxid{log_idx};
  timestamp_ = std::max(time, timestamp_);

  erase_expired_nodes();

  if (close_session(request)) {
    Session<Path> tmp{
        .session_id = 0,
        .request_no = request.request_no - 1,
    };
    return exec(&tmp, std::move(request));
  }

  Session<Path>* session = get_session(request.session_id);
  if (!session) {
    if (!new_session(request))
      return ZkResponse{
          .state   = SessionState::kExpired,
          .status  = ResultStatus::kSessionExpired,
          .zxid    = curr_zxid_,
          .events  = {},
          .results = {},
      };
    session = new_session(INT(log_idx),
                          request.request_no,
                          curr_zxid_,
                          std::get<NewSessionCommand>(request.commands.at(0)));
    return zk_response(session);
  }
  session->last_ping_time = std::time(nullptr);

  return exec(session, std::move(request));
}

ZkResponse ZkState::exec(Session<Path>* session, ZkRequest request) {
  if (session->request_no > request.request_no) {
    return ZkResponse{
        .state   = SessionState::kConnected,
        .status  = ResultStatus::kStaleRequest,
        .zxid    = curr_zxid_,
        .events  = {},
        .results = {},
    };
  }
  if (session->request_no == request.request_no) {
    LOG_INFO << "Returning executed request";
    return zk_response(session);
  }

  session->request_no = request.request_no;
  session->status     = ResultStatus::kOk;
  session->curr_zxid  = curr_zxid_;
  session->events.clear();
  session->results.clear();

  // Trigger the watches.
  session->events = trigger_watches(session->watches, session->deletes);

  vector<UndoCommand> rollback;

  for (Command& cmd : request.commands) {
    auto [result, undo] = std::visit(
        [this, session](auto&& c) -> std::tuple<CommandResult, UndoCommand> {
          return exec(session, std::move(c));
        },
        cmd);

    rollback.emplace_back(std::move(undo));
    session->results.emplace_back(std::move(result));

    session->status = session->results.back().status;
    if (session->results.back().status != ResultStatus::kOk) {
      for (auto iter = rollback.rbegin(); iter != rollback.rend(); ++iter) {
        (*iter)(session);
      }
      break;
    }
  }
  return zk_response(session);
}

ZkResponse ZkState::local_exec(Session<Path>* session, ZkRequest request) const {
  if (session->request_no > request.request_no) {
    return ZkResponse{
        .state   = SessionState::kConnected,
        .status  = ResultStatus::kStaleRequest,
        .zxid    = curr_zxid_,
        .events  = {},
        .results = {},
    };
  }
  if (session->request_no == request.request_no) {
    return zk_response(session);
  }

  session->request_no = request.request_no;
  session->status     = ResultStatus::kOk;
  session->events.clear();
  session->results.clear();

  // Trigger the watches.
  session->events = trigger_watches(session->watches, session->deletes);

  for (Command& cmd : request.commands) {
    if (std::holds_alternative<GetValueCommand>(cmd))
      session->results.emplace_back(exec(std::get<GetValueCommand>(cmd)));
    else if (std::holds_alternative<NodeExistsCommand>(cmd))
      session->results.emplace_back(exec(std::get<NodeExistsCommand>(cmd)));
    else if (std::holds_alternative<GetChildrenCommand>(cmd))
      session->results.emplace_back(exec(std::get<GetChildrenCommand>(cmd)));
    else
      session->results.emplace_back(CommandResult{
          .status = ResultStatus::kProgramBug,
          .result = std::monostate{},
      });

    session->status = session->results.back().status;
    if (session->results.back().status != ResultStatus::kOk)
      break;
  }
  return zk_response(session);
}

ZkResponse ZkState::zk_response(Session<Path> const* session) const {
  return ZkResponse{
      .status  = session->status,
      .state   = SessionState::kConnected,
      .zxid    = session->curr_zxid,
      .events  = session->events,
      .results = session->results,
  };
}

ZkState::Session<ZkState::Path>* ZkState::new_session(uint64_t                 log_idx,
                                                      int64_t                  request_no,
                                                      Zxid                     curr_zxid,
                                                      NewSessionCommand const& cmd) {
  /* create root node */
  if (data_.empty())
    data_.emplace(
        Path{}, NodeData{.data = 0, .stat = Stat{.mzxid = Zxid{0}, .czxid = Zxid{0}}, .value = {}});

  auto const session_id = log_idx;

  auto pos =
      sessions_
          .emplace(
              session_id,
              Session<Path>{
                  .session_id      = session_id,
                  .timeout_seconds = cmd.timeout_seconds,
                  .last_ping_time  = std::time(nullptr),
                  .request_no      = request_no,
                  .curr_zxid       = curr_zxid,
                  .ephemeral       = {},
                  .watches         = {},
                  .events          = {},
                  .results         = {CommandResult{.status = ResultStatus::kOk,
                                                    .result = NewSessionResult{.session_id = session_id}}},
              })
          .first;

  return &(pos->second);
}

ZkState::Session<ZkState::Path>* ZkState::get_session(uint64_t session_id) {
  if (auto pos = sessions_.find(session_id); pos != sessions_.end()) {
    return &(pos->second);
  }
  return nullptr;
}

ZkState::Session<ZkState::Path> const* ZkState::get_session(uint64_t session_id) const {
  if (auto pos = sessions_.find(session_id); pos != sessions_.end()) {
    return &(pos->second);
  }
  return nullptr;
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>*,
                                                              NewSessionCommand) {
  assert(false);
  return std::make_tuple(
      CommandResult{
          .status = ResultStatus::kProgramBug,
          .result = std::monostate{},
      },
      noop);
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>*,
                                                              CloseSessionCommand cmd) {
  Session<Path>* session = get_session(cmd.session_id);
  if (!session)
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kOk,
            .result = CloseSessionResult{},
        },
        noop);

  auto ephemeral = session->ephemeral;
  for (Path const& path : ephemeral) {
    if (auto pos = data_.find(path); pos != data_.end()) {
      if (pos->second.frozen())
        continue;

      auto [result, _] = exec(nullptr, EraseNodeCommand{.path = to_string(path)});
      assert(result.status == ResultStatus::kOk);
    }
  }
  sessions_.erase(session->session_id);
  auto r = std::make_tuple(
      CommandResult{
          .status = ResultStatus::kOk,
          .result = CloseSessionResult{},
      },
      noop);
  return r;
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>*,
                                                              IncCounterCommand cmd) {
  counter_ += cmd.increment;

  auto result =
      CommandResult{.status = ResultStatus::kOk, .result = IncCounterResult{.value = counter_}};

  auto undo = [this, cmd](Session<Path>*) { counter_ -= cmd.increment; };

  return std::make_tuple(std::move(result), std::move(undo));
}

CommandResult ZkState::exec(GetValueCommand const& cmd) const {
  if (cmd.path.empty() || cmd.path.front() != '/' || cmd.path.back() == '/')
    return CommandResult{
        .status = ResultStatus::kNoNode,
        .result = std::monostate{},
    };

  std::optional<Path> path = to_path(cmd.path);
  if (!path)
    return CommandResult{
        .status = ResultStatus::kNoNode,
        .result = std::monostate{},
    };

  auto pos = data_.find(*path);
  return CommandResult{
      .status = ResultStatus::kOk,
      .result = GetValueResult{.value = pos->second.value, .stat = pos->second.stat}};
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>* session,
                                                              GetValueCommand         cmd) {
  auto result = exec(cmd);
  if (result.status != ResultStatus::kOk)
    return std::make_tuple(std::move(result), noop);

  if (cmd.watch) {
    auto [pos, inserted] = session->watches.emplace(
        std::make_tuple(WatchEventType::kNodeEvent, *to_path(cmd.path)), curr_zxid_);
    // The iterator is valid on undo (command processing doesn't delete watches).
    auto undo = [pos, inserted](Session<Path>* session) {
      if (inserted)
        session->watches.erase(pos);
    };
    return std::make_tuple(std::move(result), std::move(undo));
  } else
    return std::make_tuple(std::move(result), noop);
}

// The path is in the subtree.
bool ZkState::is_subtree(Path const& path, Path const& p) {
  if (path.size() >= p.size())
    return false;

  auto iter = p.begin();
  return std::accumulate(path.begin(), path.end(), true, [&iter](bool result, PathId id) {
    return result && id == (*iter++);
  });
};

CommandResult ZkState::exec(GetChildrenCommand const& cmd) const {
  if (cmd.path.empty() || cmd.path.front() != '/' || cmd.path.back() == '/')
    return CommandResult{
        .status = ResultStatus::kNoNode,
        .result = std::monostate{},
    };

  std::optional<Path> path = to_path(cmd.path);
  if (!path)
    return CommandResult{
        .status = ResultStatus::kNoNode,
        .result = std::monostate{},
    };

  auto pos      = data_.find(*path);
  auto children = vector<string>{};

  ++pos;
  for (; pos != data_.end(); ++pos) {
    if (pos->first.size() <= path->size())
      break;

    if (!is_subtree(*path, pos->first))
      break;

    auto iter = pos->first.begin() + i8(path->size());
    auto end  = pos->first.end();

    PathSegment seg = segment(iter, end);
    if (iter == end)
      children.emplace_back(to_string(seg));
  }

  std::sort(children.begin(), children.end());

  return CommandResult{
      .status = ResultStatus::kOk,
      .result = GetChildrenResult{
          .path = cmd.path, .children = std::move(children), .stat = pos->second.stat}};
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>* session,
                                                              GetChildrenCommand      cmd) {
  auto result = exec(cmd);
  if (result.status != ResultStatus::kOk)
    return {std::move(result), noop};

  if (cmd.watch) {
    auto [pos, inserted] = session->watches.emplace(
        std::make_tuple(WatchEventType::kChildEvent, *to_path(cmd.path)), curr_zxid_);

    // The iterator is valid on undo (command processing doesn't delete watches).
    auto undo = [pos, inserted](Session<Path>* session) {
      if (inserted)
        session->watches.erase(pos);
    };
    return std::make_tuple(std::move(result), std::move(undo));
  } else
    return std::make_tuple(std::move(result), noop);
}

CommandResult ZkState::exec(NodeExistsCommand const& cmd) const {
  if (!cmd.path.empty() && (cmd.path.front() != '/' || cmd.path.back() == '/'))
    return CommandResult{
        .status = ResultStatus::kNoNode,
        .result = std::monostate{},
    };

  std::optional<Path> path = to_path(cmd.path);
  if (!path)
    return CommandResult{
        .status = ResultStatus::kNoNode,
        .result = std::monostate{},
    };

  NodeData const& node = data_.find(*path)->second;

  if (cmd.zxid && node.stat.mzxid != cmd.zxid.value())
    return CommandResult{
        .status = ResultStatus::kBadVersion,
        .result = std::monostate{},
    };

  return CommandResult{.status = ResultStatus::kOk, .result = NodeExistsResult{.stat = node.stat}};
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>*,
                                                              NodeExistsCommand cmd) {
  return {exec(cmd), noop};
}

vector<Zxid> ZkState::update_zxid(Path path, unsigned depth) {
  vector<Zxid> result;
  result.reserve(std::min<size_t>(path.size(), depth));

  update<false>(path, result, depth);
  return result;
}

template <bool parent>
void ZkState::update(Path& path, vector<Zxid>& old_values, unsigned depth) {
  if (old_values.size() == depth)
    return;

  auto pos = data_.find(path);
  if constexpr (parent) {
    old_values.emplace_back(pos->second.stat.czxid);
    pos->second.stat.czxid = curr_zxid_;
  } else {
    old_values.emplace_back(pos->second.stat.mzxid);
    pos->second.stat.mzxid = curr_zxid_;
  }

  if (path.empty())
    return;

  auto pid = pop(path);
  update<true>(path, old_values, depth);
  push(path, std::move(pid));
}

template <bool parent>
void ZkState::rollback(Path& path, vector<Zxid>& old_values) {
  if (old_values.empty())
    return;

  auto pos = data_.find(path);
  if constexpr (parent) {
    pos->second.stat.czxid = old_values.front();
  } else {
    pos->second.stat.mzxid = old_values.front();
  }
  old_values.erase(old_values.begin());

  if (old_values.empty())
    return;

  auto pid = pop(path);
  rollback<true>(path, old_values);
  push(path, std::move(pid));
}

void ZkState::rollback_zxid(Path path, vector<Zxid> old_values) {
  rollback<false>(path, old_values);
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>*,
                                                              SetValueCommand cmd) {
  if (!cmd.path.empty() && (cmd.path.front() != '/' || cmd.path.back() == '/'))
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kInvalidNodePath,
            .result = std::monostate{},
        },
        noop);

  if (!is_valid_value(cmd.value))
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kInvalidNodeValue,
            .result = std::monostate{},
        },
        noop);

  std::optional<Path> path = to_path(cmd.path);
  if (!path)
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kNoNode,
            .result = std::monostate{},
        },
        noop);

  NodeData& node = data_.find(*path)->second;

  if (cmd.zxid && node.stat.mzxid != cmd.zxid.value())
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kBadVersion,
            .result = std::monostate{},
        },
        noop);

  string value = std::move(node.value);
  node.value   = std::move(cmd.value);

  auto zxids = update_zxid(*path, 1U);

  auto undo = [this, value = std::move(value), path = std::move(*path), zxids = std::move(zxids)](
                  Session<Path>*) {
    data_.find(path)->second.value = std::move(value);
    rollback_zxid(path, std::move(zxids));
  };

  return std::make_tuple(
      CommandResult{.status = ResultStatus::kOk, .result = SetValueResult{.stat = node.stat}},
      std::move(undo));
}

std::tuple<string, string> ZkState::parent_and_node(string const& path) {
  assert(path.at(0) == '/');

  auto pos = path.rfind('/');

  return std::make_tuple(path.substr(0, pos), path.substr(pos + 1));
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>* session,
                                                              CreateNodeCommand       cmd) {
  if (!is_valid_path(cmd.path, cmd.sequential))
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kInvalidNodePath,
            .result = std::monostate{},
        },
        noop);
  if (!is_valid_value(cmd.value))
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kInvalidNodeValue,
            .result = std::monostate{},
        },
        noop);
  if (cmd.path.empty() || cmd.path.front() != '/' || cmd.path.back() == '/')
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kInvalidNodePath,
            .result = std::monostate{},
        },
        noop);

  // Check parent's existence.
  auto const [pnode, node] = parent_and_node(cmd.path);

  std::optional<Path> ppath = to_path(pnode);
  if (!ppath)
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kNoNode,
            .result = std::monostate{},
        },
        noop);

  // No children to sequential or ephemeral nodes.
  NodeData& pdata = data_.find(*ppath)->second;

  if (pdata.frozen())
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kNodeFrozen,
            .result = std::monostate{},
        },
        noop);
  if (pdata.ephemeral())
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kNodeEphemeral,
            .result = std::monostate{},
        },
        noop);

  Path path = *ppath;

  // Node itself exists?
  {
    if (!cmd.sequential) {
      if (append(path, node)) {
        return cmd.if_not_exists
                   ? std::make_tuple(CommandResult{.status = ResultStatus::kOk,
                                                   .result =
                                                       CreateNodeResult{
                                                           .path = to_string(path),
                                                           .stat = data_.find(path)->second.stat,
                                                       }},
                                     noop)
                   : std::make_tuple(
                         CommandResult{
                             .status = ResultStatus::kNodeExists,
                             .result = std::monostate{},
                         },
                         noop);
      } else if (auto seg = try_num(node); seg)
        push(path, *seg);
      else
        push(path, PathSegment{str_id(node)});
    } else {
      static_assert(NodeData::kSequenceMask > kSequenceThreshold);

      uint64_t seq = data_.find(*ppath)->second.sequence() + 1U;

      if (seq >= kSequenceThreshold)
        return std::make_tuple(
            CommandResult{
                .status = ResultStatus::kSequenceOverflow,
                .result = std::monostate{},
            },
            noop);

      PathId id     = str_id(node);
      auto [n1, n2] = split<true>(seq);

      PathSegment seg{id, n1, n2};

      if (append(path, to_string(seg))) {
        return cmd.if_not_exists
                   ? std::make_tuple(CommandResult{.status = ResultStatus::kOk,
                                                   .result =
                                                       CreateNodeResult{
                                                           .path = to_string(path),
                                                           .stat = data_.find(path)->second.stat,
                                                       }},
                                     noop)
                   : std::make_tuple(
                         CommandResult{
                             .status = ResultStatus::kNodeExists,
                             .result = std::monostate{},
                         },
                         noop);
      } else
        push(path, seg);
    }
  }

  if (cmd.sequential)
    data_.find(*ppath)->second.inc_sequence();
  //++(sequence_nodes_.emplace(*ppath, 0).first->second);

  auto [pos, _] = data_.emplace(path,
                                NodeData{
                                    .data = NodeData::make_data(cmd.sequential, cmd.ephemeral),
                                    .stat =
                                        Stat{
                                            .mzxid = curr_zxid_,
                                            .czxid = curr_zxid_,
                                        },
                                    .value = cmd.value,
                                });
  auto result   = CommandResult{.status = ResultStatus::kOk,
                                .result = CreateNodeResult{
                                    .path = to_string(path),
                                    .stat = pos->second.stat,
                              }};

  auto zxids = update_zxid(path, 2U);

  if (cmd.ephemeral)
    session->ephemeral.emplace(path);

  auto undo =
      [this, path, ppath = std::move(*ppath), seq = cmd.sequential, zxids = std::move(zxids)](
          Session<Path>* session) {
        if (seq)
          data_.find(ppath)->second.dec_sequence();
        session->ephemeral.erase(path);

        rollback_zxid(path, std::move(zxids));
        data_.erase(path);
      };
  return std::make_tuple(std::move(result), std::move(undo));
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>*,
                                                              ExtendTTLCommand cmd) {
  std::optional<Path> path = to_path(cmd.path);
  if (!path)
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kNoNode,
            .result = std::monostate{},
        },
        noop);

  NodeData& node = data_.find(*path)->second;
  if (!node.frozen())
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kNoTTLNode,
            .result = std::monostate{},
        },
        noop);

  auto old_expiry = cmd.expiry;
  auto new_expiry = timestamp_ + cmd.ttl_secs;

  auto key = std::make_tuple(old_expiry, *path);
  if (auto pos = ttl_nodes_.find(key); pos == ttl_nodes_.end())
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kInvalidExpiry,
            .result = std::monostate{},
        },
        noop);
  else
    ttl_nodes_.erase(pos);

  ttl_nodes_.emplace(new_expiry, *path);
  auto undo = [this, new_expiry, old_expiry, path = std::move(*path)](Session<Path>*) {
    ttl_nodes_.erase(std::make_tuple(new_expiry, path));
    ttl_nodes_.emplace(old_expiry, std::move(path));
  };

  return std::make_tuple(
      CommandResult{.status = ResultStatus::kOk, .result = ExtendTTLResult{.expiry = new_expiry}},
      std::move(undo));
}

std::tuple<CommandResult, ZkState::UndoCommand> ZkState::exec(ZkState::Session<Path>*,
                                                              EraseNodeCommand cmd) {
  if (cmd.path.empty() || cmd.path.front() != '/' || cmd.path.back() == '/')
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kInvalidNodePath,
            .result = std::monostate{},
        },
        noop);

  std::optional<Path> path = to_path(cmd.path);
  if (!path)
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kNoNode,
            .result = std::monostate{},
        },
        noop);

  for (auto pos = data_.find(*path); pos != data_.end(); ++pos)
    if (pos->first != *path && is_subtree(*path, pos->first))
      return std::make_tuple(
          CommandResult{
              .status = ResultStatus::kHasChildren,
              .result = std::monostate{},
          },
          noop);

  if (cmd.zxid && data_.find(*path)->second.stat.mzxid != cmd.zxid.value())
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kBadVersion,
            .result = std::monostate{},
        },
        noop);

  if (cmd.ttl_secs) {
    NodeData& node = data_.find(*path)->second;

    if (node.frozen())
      return std::make_tuple(
          CommandResult{
              .status = ResultStatus::kNodeFrozen,
              .result = std::monostate{},
          },
          noop);

    std::time_t expiry = timestamp_ + cmd.ttl_secs.value();
    node.set_frozen(true);
    ttl_nodes_.emplace(expiry, *path);

    auto undo = [this, expiry, path = std::move(*path)](Session<Path>*) {
      NodeData& node = data_.find(path)->second;

      node.set_frozen(false);
      ttl_nodes_.erase(std::make_tuple(expiry, path));
    };
    return std::make_tuple(
        CommandResult{.status = ResultStatus::kOk, .result = EraseNodeResult{.expiry = expiry}},
        std::move(undo));
  } else if (data_.find(*path)->second.frozen())
    return std::make_tuple(
        CommandResult{
            .status = ResultStatus::kNodeFrozen,
            .result = std::monostate{},
        },
        noop);

  auto zxids = update_zxid(*path, 2U);
  auto node  = std::move(data_.find(*path)->second);
  data_.erase(*path);

  vector<std::function<void()>> undo_session_changes;
  undo_session_changes.reserve(sessions_.size() + 1U);

  if (node.ephemeral()) {
    for (auto& [_, session] : sessions_) {
      if (session.ephemeral.erase(*path) > 0) {
        undo_session_changes.emplace_back(
            [&session, path = *path]() { session.ephemeral.emplace(path); });
        break;
      }
    }
  }

  for (auto& [_, session] : sessions_) {
    auto has_watch = [](Session<Path> const& session, Path const& path) {
      for (auto type : watch_event_types()) {
        auto key = std::make_tuple(type, path);

        if (session.watches.find(key) != session.watches.end())
          return true;
      }
      return false;
    };

    if (!has_watch(session, *path))
      continue;

    auto [pos, inserted] = session.deletes.emplace(*path);

    // The iterator is valid on undo (command processing doesn't delete session data).
    undo_session_changes.emplace_back([&session, pos, inserted]() {
      if (inserted)
        session.deletes.erase(pos);
    });
  }

  auto undo = [this,
               zxids                = std::move(zxids),
               node                 = std::move(node),
               path                 = std::move(*path),
               undo_session_changes = std::move(undo_session_changes)](Session<Path>*) {
    std::for_each(
        undo_session_changes.begin(), undo_session_changes.end(), [](auto& func) { func(); });
    data_.emplace(path, std::move(node));
    rollback_zxid(path, std::move(zxids));
  };

  return std::make_tuple(
      CommandResult{
          .status = ResultStatus::kOk,
          .result = std::monostate{},
      },
      std::move(undo));
}

vector<Command> ZkState::expire_sessions() const {
  auto lk = shared_lock();

  auto now = epoch_msec();
  if (last_session_expiry_check_.load() + kSessionExpiryCheck > now)
    return {};
  last_session_expiry_check_.store(now);

  vector<Command> commands;

  auto tstamp = std::time(nullptr);
  for (auto const& [session_id, session] : sessions_) {
    auto lk = session_lock(session_id);
    if (session.last_ping_time + session.timeout_seconds < tstamp)
      commands.emplace_back(CloseSessionCommand{.session_id = session_id});
  }
  return commands;
}

bool ZkState::operator==(ZkState const& that) const {
  return data() == that.data() && ttl_nodes() == that.ttl_nodes() &&
         sessions() == that.sessions() && curr_zxid_ == that.curr_zxid_ &&
         timestamp_ == that.timestamp_ && counter_ == that.counter_;
}

map<uint64_t, ZkState::Session<string>> ZkState::sessions() const {
  auto func = [this](Path const& p) -> string { return to_string(p); };

  return std::accumulate(sessions_.begin(),
                         sessions_.end(),
                         map<uint64_t, Session<string>>{},
                         [func](auto result, std::pair<uint64_t, Session<Path>> const& elem) {
                           result.emplace(elem.first, elem.second.copy<string>(func));
                           return result;
                         });
}

set<std::tuple<std::time_t, string>> ZkState::ttl_nodes() const {
  auto result = std::accumulate(ttl_nodes_.begin(),
                                ttl_nodes_.end(),
                                set<std::tuple<std::time_t, string>>{},
                                [this](auto result, std::tuple<std::time_t, Path> const& elem) {
                                  result.emplace(std::get<std::time_t>(elem),
                                                 to_string(std::get<Path>(elem)));
                                  return result;
                                });
  assert(result.size() == ttl_nodes_.size());
  return result;
}

map<string, ZkState::NodeData> ZkState::data() const {
  auto result = std::accumulate(data_.begin(),
                                data_.end(),
                                map<string, NodeData>{},
                                [this](auto result, std::pair<Path const, NodeData> const& elem) {
                                  result.emplace(to_string(elem.first), elem.second);
                                  return result;
                                });
  assert(result.size() == data_.size());
  return result;
}

set<string> ZkState::strings() const {
  return std::accumulate(path_seg_.begin(),
                         path_seg_.end(),
                         set<string>{},
                         [](set<string> result, string_view const& n) {
                           auto [_, inserted] = result.emplace(n);

                           (void)inserted;
                           assert(inserted);

                           return result;
                         });
}

std::ostream& operator<<(std::ostream& out, ZkState const& state) {
  out << "zxid     : " << state.curr_zxid_ << '\n';
  out << "timestamp: " << state.timestamp_ << '\n';
  out << "counter  : " << state.counter_ << '\n';
  {
    map<string, ZkState::NodeData> data = state.data();

    auto b = [](bool p) -> char const* { return p ? "true" : "false"; };

    out << "tree: " << data.size() << '\n';
    for (auto const& [path, node] : data) {
      out << "  " << path << '\n';
      out << "    flags  = (" << b(node.sequential()) << ", " << b(node.ephemeral()) << ", "
          << b(node.frozen()) << ")\n";
      out << "    seq    = " << node.sequence() << '\n';
      out << "    stat   = " << node.stat << '\n';
      out << "    value  = " << node.value << '\n';
    }
  }
  {
    out << "ttl nodes: " << state.ttl_nodes_.size() << '\n';
    for (auto const& [time, path] : state.ttl_nodes())
      out << "  " << path << " = " << time << '\n';
  }
  {
    out << "sessions: " << state.sessions_.size() << '\n';
    for (auto const& [_, session] : state.sessions()) {
      out << "  session id  : " << session.session_id << '\n';
      out << "  timeout secs: " << session.timeout_seconds << '\n';
      out << "  ping time   : " << session.last_ping_time << '\n';
      out << "  request no  : " << session.request_no << '\n';
      out << "  events      : " << session.events.size() << '\n';
      out << "  results     : " << session.results.size() << '\n';
      out << "  deletes     : " << session.deletes.size() << '\n';
      out << "  ephemeral   : " << session.ephemeral.size() << '\n';
      out << "  watches     : " << session.watches.size() << '\n';
    }
  }
  return out;
}

bool ZkState::verify_rollback(uint64_t session_id, ZkState const& before) const {
  auto fail = [this, &before](char const* message) -> bool {
    LOG_ERROR << message;
    LOG_ERROR << "Before " << before;
    LOG_ERROR << "After " << *this;
    return false;
  };

  if (data_ != before.data_)
    return fail("The data tree has been modified");

  if (ttl_nodes_ != before.ttl_nodes_)
    return fail("The ttl nodes are different");

  for (auto const& [id, session] : sessions_) {
    if (id != session_id) {
      if (session != before.sessions_.find(id)->second)
        return fail("The session data is different");
    } else {
      if (session.ephemeral != before.sessions_.find(id)->second.ephemeral)
        return fail("The ephemeral are different");
      if (!session.deletes.empty())
        return fail("The deletes are still here");

      auto watches = before.sessions_.find(id)->second.watches;
      auto deletes = before.sessions_.find(id)->second.deletes;

      auto events = before.trigger_watches(watches, deletes);

      if (session.watches != watches)
        return fail("The watches are not consistent");
      if (session.events != events)
        return fail("The events are not consistent");
    }
  }

  return true;
}

