#define BOOST_TEST_MODULE raft_log_snaphshot
#include "src/base/testing.h"
#include "src/raft/raft_log.h"
#include "src/raft/zk_state.h"

namespace {
ZkState clone(ZkState const& x) {
  return x;
}

/*
void echo(set<string> s) {
  std::cout << s.size() << " ";
  for (auto str : s)
    std::cout << str << ",";
  std::cout << std::endl;
}
*/

ZkResponse exec(TmpDir& dir, ZkState& state, std::time_t time, RaftIndex idx, ZkRequest request) {
  auto response = state.exec(time, idx, std::move(request));

  RaftLog log(dir());

  BOOST_TEST(log.open_logs_db().has_error() == false);
  BOOST_TEST(log.save_snapshot(state, 10).has_error() == false);

  auto copy = log.load_snapshot().value();
  BOOST_TEST(copy == state);

  return response;
}
}  // namespace

BOOST_AUTO_TEST_CASE(create_node_seq) {
  TmpDir  dir;
  ZkState state;

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  // Create a node "/top".
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{.path = "/top", .value = "test"}}});

  // Create a sequential node "/top", should result in a node "/top0000000000000000001"
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{
                                  .path = "/top", .value = "test", .sequential = true}}});

  // The root node's sequence is 1 so 2 should be created.
  // Create a sequential node "/t", should result in a node "/t0000000000000000002"
  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{
               .session_id = session_id,
               .request_no = request_no++,
               .commands = {CreateNodeCommand{.path = "/t", .value = "test", .sequential = true}}});

  // Creating a node with zero length name is invalid.
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{.path = "/top/", .value = "test"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{
                                  .path = "/top/", .value = "test", .sequential = true}}});

  // Create a sequential node "/top/t", should create "/top/t0000000000000000001"
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{
                                  .path = "/top/t", .value = "test", .sequential = true}}});

  // Create a sequential node "/t0000000000000000002", where "/t0000000000000000002" is an existing
  // sequential node.
  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = session_id,
                     .request_no = request_no++,
                     .commands   = {CreateNodeCommand{
                           .path = "/t0000000000000000002", .value = "test", .sequential = true}}});

  // Create a child node to a sequential node "/t0000000000000000002".
  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = session_id,
                     .request_no = request_no++,
                     .commands   = {CreateNodeCommand{
                           .path = "/t0000000000000000002/t", .value = "test", .sequential = true}}});

  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {CreateNodeCommand{
                      .path = "/t0000000000000000002/t-", .value = "test", .sequential = true}}});

  // Create a sub node to a sequential node "/t0000000000000000002/t-".
  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {CreateNodeCommand{
                      .path = "/t0000000000000000002/t-", .value = "test", .sequential = false}}});

  // Create a child node to a sequential node "/top0000000000000000001/child"
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands = {CreateNodeCommand{.path  = "/top0000000000000000001/child",
                                                           .value = "test",
                                                           .sequential = false}}});

  // Create a child node to a non-sequential node "/top/child"
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{
                                  .path = "/top/child", .value = "test", .sequential = false}}});
}

BOOST_AUTO_TEST_CASE(create_node_numbers) {
  TmpDir  dir;
  ZkState state;

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  // Create a node "/top".
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{.path = "/top", .value = "test"}}});

  // Create a node "/top/<kSequenceThreshold - 1>".
  string n1 = fn::format("/top/{}", ZkState::kSequenceThreshold - 1U);
  response  = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                             .request_no = request_no++,
                             .commands   = {CreateNodeCommand{.path = n1, .value = "test"}}});

  string n2 = fn::format("/top/{}", ZkState::kSequenceThreshold);
  response  = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                             .request_no = request_no++,
                             .commands   = {CreateNodeCommand{.path = n2, .value = "test"}}});

  string c1 = fn::format("{0}/{1}", n1, ZkState::kSequenceThreshold);
  response  = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                             .request_no = request_no++,
                             .commands   = {CreateNodeCommand{.path = c1, .value = "test"}}});

  string c2 = fn::format("{0}/{1}", n2, ZkState::kSequenceThreshold - 1U);
  response  = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                             .request_no = request_no++,
                             .commands   = {CreateNodeCommand{.path = c2, .value = "test"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {SetValueCommand{.path = c1, .value = "test2"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {SetValueCommand{.path = c2, .value = "test2"}}});
}

BOOST_AUTO_TEST_CASE(create_node_failures) {
  TmpDir  dir;
  ZkState state;

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  // Empty path
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{.path = "", .value = "test"}}});

  // path not starting with '/'
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{.path = "test", .value = "test"}}});

  // value having some special chars
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{.path = "/test", .value = "te\nst"}}});

  // No parent
  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = session_id,
                     .request_no = request_no++,
                     .commands   = {CreateNodeCommand{.path = "/test/child", .value = "test"}}});

  // Parent can't be ephemeral
  {
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands   = {CreateNodeCommand{
                                    .path = "/ephemeral", .value = "test", .ephemeral = true}}});

    response = exec(
        dir,
        state,
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands   = {CreateNodeCommand{.path = "/ephemeral/child", .value = "test"}}});
  }

  // Parent can't be frozen
  {
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands = {CreateNodeCommand{.path = "/frozen", .value = "test"}}});

    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands = {EraseNodeCommand{.path = "/frozen", .ttl_secs = 200}}});

    response =
        exec(dir,
             state,
             std::time(nullptr),
             RaftIndex{++log_idx},
             ZkRequest{.session_id = session_id,
                       .request_no = request_no++,
                       .commands = {CreateNodeCommand{.path = "/frozen/child", .value = "test"}}});
  }

  // Length of any segment should be less than or equal to ZkState::kSegmentSizeLimit
  // (64)
  {
    string seg{"/"};
    while (seg.size() <= ZkState::kSegmentSizeLimit)
      seg += 'a';

    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands   = {CreateNodeCommand{.path = seg, .value = "test"}}});

    response = exec(
        dir,
        state,
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{
            .session_id = session_id,
            .request_no = request_no++,
            .commands = {CreateNodeCommand{.path = "/top", .value = "test", .if_not_exists = true},
                         CreateNodeCommand{.path = string{"/top"} + seg, .value = "test"}}});

    seg += 'b';
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands   = {CreateNodeCommand{.path = seg, .value = "test"}}});

    response = exec(
        dir,
        state,
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands = {CreateNodeCommand{.path = string{"/top"} + seg, .value = "test"}}});
  }

  // Length of any segment should be less than or equal to ZkState::kSegmentSizeLimit
  // (64) so for sequential the prefix limit is ZkState::kSegmentSizeLimit -
  // ZkState::kSequenceDigits.
  {
    string seg{"/"};
    while (seg.size() <= (ZkState::kSegmentSizeLimit - ZkState::kSequenceDigits))
      seg += 'a';

    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands   = {CreateNodeCommand{
                                    .path = seg, .value = "test", .sequential = true}}});

    response =
        exec(dir,
             state,
             std::time(nullptr),
             RaftIndex{++log_idx},
             ZkRequest{.session_id = session_id,
                       .request_no = request_no++,
                       .commands   = {CreateNodeCommand{
                             .path = string{"/top"} + seg, .value = "test", .sequential = true}}});

    seg += 'b';
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands   = {CreateNodeCommand{
                                    .path = seg, .value = "test", .sequential = true}}});

    response =
        exec(dir,
             state,
             std::time(nullptr),
             RaftIndex{++log_idx},
             ZkRequest{.session_id = session_id,
                       .request_no = request_no++,
                       .commands   = {CreateNodeCommand{
                             .path = string{"/top"} + seg, .value = "test", .sequential = true}}});
  }

  // Node exits.
  {
    response =
        exec(dir,
             state,
             std::time(nullptr),
             RaftIndex{++log_idx},
             ZkRequest{.session_id = session_id,
                       .request_no = request_no++,
                       .commands   = {CreateNodeCommand{.path = "/node_exists", .value = "test"}}});

    response =
        exec(dir,
             state,
             std::time(nullptr),
             RaftIndex{++log_idx},
             ZkRequest{.session_id = session_id,
                       .request_no = request_no++,
                       .commands   = {CreateNodeCommand{
                             .path = "/node_exists/seq", .value = "test", .sequential = true}}});

    response =
        exec(dir,
             state,
             std::time(nullptr),
             RaftIndex{++log_idx},
             ZkRequest{.session_id = session_id,
                       .request_no = request_no++,
                       .commands = {CreateNodeCommand{.path = "/node_exists/seq0000000000000000001",
                                                      .value = "test"}}});

    response =
        exec(dir,
             state,
             std::time(nullptr),
             RaftIndex{++log_idx},
             ZkRequest{.session_id = session_id,
                       .request_no = request_no++,
                       .commands = {CreateNodeCommand{.path = "/node_exists/seq0000000000000000002",
                                                      .value = "test"}}});

    response =
        exec(dir,
             state,
             std::time(nullptr),
             RaftIndex{++log_idx},
             ZkRequest{.session_id = session_id,
                       .request_no = request_no++,
                       .commands   = {CreateNodeCommand{
                             .path = "/node_exists/seq", .value = "test", .sequential = true}}});

    // .if_not_exists
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands   = {CreateNodeCommand{
                                    .path          = "/node_exists/seq0000000000000000001",
                                    .value         = "test",
                                    .if_not_exists = true,
                              }}});

    // On success should the counter be incremented ?
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands   = {CreateNodeCommand{
                                    .path          = "/node_exists/seq",
                                    .value         = "test",
                                    .sequential    = true,
                                    .if_not_exists = true,
                              }}});

    // On success should the counter be incremented ? Assumption is NO.
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id,
                              .request_no = request_no++,
                              .commands   = {CreateNodeCommand{
                                    .path       = "/node_exists/seq",
                                    .value      = "test",
                                    .sequential = true,
                              }}});
  }
}

BOOST_AUTO_TEST_CASE(new_node_rollback) {
  TmpDir  dir;
  ZkState state;

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{
          .session_id = session_id,
          .request_no = request_no++,
          .commands   = {CreateNodeCommand{.path = "/test", .value = "test"},
                         CreateNodeCommand{.path = "/test/seq", .value = "test", .sequential = true},
                         CreateNodeCommand{.path = "/test/child", .value = "test"},
                         CreateNodeCommand{.path = "/test/child/gchild", .value = "test"}}});

  // Create a new session.
  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = 0,
                            .request_no = request_no++,
                            .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  auto s = clone(state);

  vector<Command> commands = {
      CreateNodeCommand{.path = "/test/t", .value = "test", .sequential = true},
      CreateNodeCommand{.path = "/test/t", .value = "test", .ephemeral = true},
      CreateNodeCommand{.path = "/test/x", .value = "test", .ephemeral = true, .sequential = true},
      CreateNodeCommand{.path = "/test/child/gchild/t", .value = "test", .sequential = true},
      CreateNodeCommand{.path = "/test/child/gchild/t", .value = "test", .ephemeral = true},
      CreateNodeCommand{
          .path = "/test/child/gchild/x", .value = "test", .ephemeral = true, .sequential = true},
      CreateNodeCommand{.path = "/test/child/gchild/y", .value = "test"},
      NodeExistsCommand{.path = "/non-existent"}};

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{
                      .session_id = session_id,
                      .request_no = request_no++,
                      .commands   = commands,
                  });
}

BOOST_AUTO_TEST_CASE(create_node_stat) {
  TmpDir  dir;
  ZkState state;

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{
          .session_id = session_id,
          .request_no = request_no++,
          .commands   = {CreateNodeCommand{.path = "/test", .value = "test"},
                         CreateNodeCommand{.path = "/test/seq", .value = "test", .sequential = true},
                         CreateNodeCommand{.path = "/test/123", .value = "test"},
                         CreateNodeCommand{.path = "/test/child", .value = "test"},
                         CreateNodeCommand{.path = "/test/child/gchild", .value = "test"}}});

  auto zxid = Zxid{log_idx};

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});

  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{
               .session_id = session_id,
               .request_no = request_no++,
               .commands   = {
                   CreateNodeCommand{.path = "/test/seq0000000000000000001/child", .value = "test"},
               }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                CreateNodeCommand{.path = "/test/123/child", .value = "test"},
                            }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                CreateNodeCommand{.path = "/test/child/child2", .value = "test"},
                            }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});

  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = session_id,
                     .request_no = request_no++,
                     .commands   = {
                         CreateNodeCommand{.path = "/test/child/gchild/child", .value = "test"},
                     }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});
}

BOOST_AUTO_TEST_CASE(set_value_stat) {
  TmpDir  dir;
  ZkState state;

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{
          .session_id = session_id,
          .request_no = request_no++,
          .commands   = {CreateNodeCommand{.path = "/test", .value = "test"},
                         CreateNodeCommand{.path = "/test/seq", .value = "test", .sequential = true},
                         CreateNodeCommand{.path = "/test/123", .value = "test"},
                         CreateNodeCommand{.path = "/test/child", .value = "test"},
                         CreateNodeCommand{.path = "/test/child/gchild", .value = "test"}}});

  auto zxid = Zxid{log_idx};

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});

  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = session_id,
                     .request_no = request_no++,
                     .commands   = {
                         SetValueCommand{.path = "/test/seq0000000000000000001", .value = "test"},
                     }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                SetValueCommand{.path = "/test/123", .value = "test"},
                            }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                SetValueCommand{.path = "/test/child", .value = "test"},
                            }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                SetValueCommand{.path = "/test/child/gchild", .value = "test"},
                            }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});
}

BOOST_AUTO_TEST_CASE(erase_node_stat) {
  TmpDir  dir;
  ZkState state;

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{
          .session_id = session_id,
          .request_no = request_no++,
          .commands   = {CreateNodeCommand{.path = "/test", .value = "test"},
                         CreateNodeCommand{.path = "/test/seq", .value = "test", .sequential = true},
                         CreateNodeCommand{.path = "/test/123", .value = "test"},
                         CreateNodeCommand{.path = "/test/child", .value = "test"},
                         CreateNodeCommand{.path = "/test/child/gchild", .value = "test"}}});

  auto zxid = Zxid{log_idx};

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"},
                                           NodeExistsCommand{.path = "/test/child/gchild"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                EraseNodeCommand{.path = "/test/child/gchild"},
                            }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"},
                                           NodeExistsCommand{.path = "/test/child"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                EraseNodeCommand{.path = "/test/child"},
                            }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/test/123"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                EraseNodeCommand{.path = "/test/123"},
                            }});

  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = session_id,
                     .request_no = request_no++,
                     .commands   = {NodeExistsCommand{.path = "/test"},
                                    NodeExistsCommand{.path = "/test/seq0000000000000000001"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                EraseNodeCommand{.path = "/test/seq0000000000000000001"},
                            }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test"}}});
}

BOOST_AUTO_TEST_CASE(erase_failures) {
  TmpDir  dir;
  ZkState state;

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{
          .session_id = session_id,
          .request_no = request_no++,
          .commands   = {CreateNodeCommand{.path = "/test", .value = "test"},
                         CreateNodeCommand{.path = "/test/seq", .value = "test", .sequential = true},
                         CreateNodeCommand{.path = "/test/123", .value = "test"},
                         CreateNodeCommand{.path = "/test/child", .value = "test"},
                         CreateNodeCommand{.path = "/test/child/gchild", .value = "test"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {EraseNodeCommand{.path = ""}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {EraseNodeCommand{.path = "/"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {EraseNodeCommand{.path = "/test/123/"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {EraseNodeCommand{.path = "/test/12"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands = {EraseNodeCommand{.path = "/test/123", .zxid = Zxid{42}}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {EraseNodeCommand{.path = "/test/child"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {EraseNodeCommand{.path = "/test/123", .ttl_secs = 60}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {NodeExistsCommand{.path = "/test/123"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {EraseNodeCommand{.path = "/test/123"}}});
}

BOOST_AUTO_TEST_CASE(erase_success) {
  TmpDir  dir;
  ZkState state;

  uint64_t log_idx = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = 0,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id_1 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{
               .session_id = session_id_1,
               .request_no = 1,
               .commands   = {
                   CreateNodeCommand{.path = "/test", .value = "test"},
                   CreateNodeCommand{
                         .path = "/test/seq", .value = "test", .sequential = true, .ephemeral = true},
                   CreateNodeCommand{.path = "/test/123", .value = "test"},
                   CreateNodeCommand{.path = "/test/child", .value = "test"},
                   CreateNodeCommand{.path = "/test/child/gchild", .value = "test"}}});

  // Create another session.
  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = 0,
                     .commands   = {NewSessionCommand{.name = "test2", .timeout_seconds = 5}}});

  uint64_t const session_id_2 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id_2,
                .request_no = 1,
                .commands   = {GetValueCommand{.path = "/test/123", .watch = true},
                               GetChildrenCommand{.path = "/test/child/gchild", .watch = true}}});

  // Create another session.
  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = 0,
                     .commands   = {NewSessionCommand{.name = "test3", .timeout_seconds = 5}}});

  uint64_t const session_id_3 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  {
    // Erase /test/seq0000000000000000001, updates ephemeral list

    response =
        exec(dir,
             state,
             std::time(nullptr),
             RaftIndex{++log_idx},
             ZkRequest{.session_id = session_id_3,
                       .request_no = 1,
                       .commands   = {EraseNodeCommand{.path = "/test/seq0000000000000000001"}}});
  }

  {
    // Erase /test/123 update deletes list

    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id_3,
                              .request_no = 2,
                              .commands   = {EraseNodeCommand{.path = "/test/123"}}});
  }

  {
    // Erase /test/child/gchild update deletes list
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id_3,
                              .request_no = 3,
                              .commands   = {EraseNodeCommand{.path = "/test/child/gchild"}}});
  }

  {
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id_3,
                              .request_no = 4,
                              .commands   = {EraseNodeCommand{.path = "/test/child"},
                                             EraseNodeCommand{.path = "/test"}}});
  }

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id_2,
                            .request_no = 2,
                            .commands   = {GetValueCommand{.path = "/test"}}});
}

BOOST_AUTO_TEST_CASE(erase_rollback) {
  TmpDir  dir;
  ZkState state;

  uint64_t log_idx = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = 0,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id_1 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{
               .session_id = session_id_1,
               .request_no = 1,
               .commands   = {
                   CreateNodeCommand{.path = "/test", .value = "test"},
                   CreateNodeCommand{
                         .path = "/test/seq", .value = "test", .sequential = true, .ephemeral = true},
                   CreateNodeCommand{.path = "/test/123", .value = "test"},
                   CreateNodeCommand{.path = "/test/child", .value = "test"},
                   CreateNodeCommand{.path = "/test/child/gchild", .value = "test"}}});

  // Create another session.
  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = 0,
                     .commands   = {NewSessionCommand{.name = "test2", .timeout_seconds = 5}}});

  uint64_t const session_id_2 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id_2,
                .request_no = 1,
                .commands   = {GetValueCommand{.path = "/test/123", .watch = true},
                               GetChildrenCommand{.path = "/test/child/gchild", .watch = true}}});

  // Create another session.
  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = 0,
                     .commands   = {NewSessionCommand{.name = "test3", .timeout_seconds = 5}}});

  uint64_t const session_id_3 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  {
    ZkState s = clone(state);

    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id_3,
                              .request_no = 1,
                              .commands = {EraseNodeCommand{.path = "/test/seq0000000000000000001"},
                                           NodeExistsCommand{.path = "/non-existent"}}});
  }

  {
    ZkState s = clone(state);
    // Erase /test/123 update deletes list
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id_3,
                              .request_no = 2,
                              .commands   = {EraseNodeCommand{.path = "/test/123"},
                                             NodeExistsCommand{.path = "/non-existent"}}});
  }

  {
    ZkState s = clone(state);

    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id_3,
                              .request_no = 3,
                              .commands   = {EraseNodeCommand{.path = "/test/child/gchild"},
                                             NodeExistsCommand{.path = "/non-existent"}}});
  }

  {
    ZkState s = clone(state);

    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id_3,
                              .request_no = 4,
                              .commands   = {EraseNodeCommand{.path = "/test/child/gchild"},
                                             EraseNodeCommand{.path = "/test/child"},
                                             EraseNodeCommand{.path = "/test/123"},
                                             EraseNodeCommand{.path = "/test/seq0000000000000000001"},
                                             EraseNodeCommand{.path = "/test"},
                                             NodeExistsCommand{.path = "/non-existent"}}});
  }
  {
    response = exec(dir,
                    state,
                    std::time(nullptr),
                    RaftIndex{++log_idx},
                    ZkRequest{.session_id = session_id_3,
                              .request_no = 5,
                              .commands   = {
                                  EraseNodeCommand{.path = "/test/child/gchild"},
                                  EraseNodeCommand{.path = "/test/child"},
                                  EraseNodeCommand{.path = "/test/123"},
                                  EraseNodeCommand{.path = "/test/seq0000000000000000001"},
                                  EraseNodeCommand{.path = "/test"},
                              }});
  }
}

BOOST_AUTO_TEST_CASE(rollback) {
  TmpDir  dir;
  ZkState state;

  uint64_t log_idx    = INT(state.curr_zxid());
  int64_t  request_no = 0;

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  auto s = clone(state);

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {CreateNodeCommand{.path = "/test", .value = "test"},
                                           SetValueCommand{.path = "/test", .value = "test2"},
                                           GetValueCommand{.path = "/test", .watch = true},
                                           GetChildrenCommand{.path = "/test", .watch = true},
                                           EraseNodeCommand{.path = "/test"},
                                           NodeExistsCommand{.path = "/non-existent"}}});
}

BOOST_AUTO_TEST_CASE(get_ops) {
  TmpDir  dir;
  ZkState state;

  uint64_t log_idx    = INT(state.curr_zxid());
  int64_t  request_no = 0;

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{
               .session_id = session_id,
               .request_no = request_no++,
               .commands   = {
                   CreateNodeCommand{.path = "/test", .value = "test"},
                   CreateNodeCommand{
                         .path = "/test/seq", .value = "test", .sequential = true, .ephemeral = true},
                   CreateNodeCommand{.path = "/test/123", .value = "test"},
                   CreateNodeCommand{.path = "/test/child", .value = "test"},
                   CreateNodeCommand{.path = "/test/child/gchild", .value = "test"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {GetValueCommand{.path = "/test"},
                                           GetChildrenCommand{.path = "/test"}}});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {
                                SetValueCommand{.path = "/test", .value = "test2"},
                                EraseNodeCommand{.path = "/test/123"},
                                CreateNodeCommand{.path = "/test/apple", .value = "x"},
                                CreateNodeCommand{.path = "/test/child/gchild2", .value = "x"},
                            }});

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id,
                            .request_no = request_no++,
                            .commands   = {GetValueCommand{.path = "/test"},
                                           GetChildrenCommand{.path = "/test"}}});
}

BOOST_AUTO_TEST_CASE(close_session) {
  TmpDir  dir;
  ZkState state;

  uint64_t log_idx    = INT(state.curr_zxid());
  int64_t  request_no = 0;

  // Create a session.
  ZkResponse response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id_1 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = exec(
      dir,
      state,
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{
          .session_id = session_id_1,
          .request_no = request_no++,
          .commands   = {
              CreateNodeCommand{.path = "/test", .value = "test"},
              CreateNodeCommand{
                    .path = "/test/seq", .value = "test_seq", .sequential = true, .ephemeral = true},
              CreateNodeCommand{.path = "/test/123", .value = "test_123"},
              CreateNodeCommand{.path = "/test/child", .value = "test_child"},
              CreateNodeCommand{
                    .path = "/test/child/gchild", .value = "test_child_gchild", .ephemeral = true}}});

  // Create another session.
  response =
      exec(dir,
           state,
           std::time(nullptr),
           RaftIndex{++log_idx},
           ZkRequest{.session_id = 0,
                     .request_no = request_no++,
                     .commands   = {NewSessionCommand{.name = "test2", .timeout_seconds = 5}}});

  uint64_t const session_id_2 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = exec(dir,
                  state,
                  std::time(nullptr),
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id_2,
                            .request_no = request_no++,
                            .commands   = {
                                NodeExistsCommand{.path = "/test/123"},
                                NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                NodeExistsCommand{.path = "/test/child/gchild"},
                            }});

  auto t = std::time(nullptr);

  response =
      exec(dir,
           state,
           t,
           RaftIndex{++log_idx},
           ZkRequest{.session_id = session_id_2,
                     .request_no = request_no++,
                     .commands   = {
                         EraseNodeCommand{.path = "/test/seq0000000000000000001", .ttl_secs = 10},
                     }});

  response = exec(dir,
                  state,
                  t,
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id_2,
                            .request_no = request_no++,
                            .commands   = {
                                NodeExistsCommand{.path = "/test/123"},
                                NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                NodeExistsCommand{.path = "/test/child/gchild"},
                            }});

  response = exec(dir,
                  state,
                  t,
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = 0,
                            .request_no = 0,
                            .commands   = {CloseSessionCommand{.session_id = session_id_1}}});

  response = exec(dir,
                  state,
                  t + 2,
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id_2,
                            .request_no = request_no++,
                            .commands   = {
                                NodeExistsCommand{.path = "/test/123"},
                                NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                NodeExistsCommand{.path = "/test/child/gchild"},
                            }});

  response = exec(
      dir,
      state,
      t + 3,
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id_2,
                .request_no = request_no++,
                .commands   = {
                    ExtendTTLCommand{
                          .path = "/test/seq0000000000000000001", .expiry = t + 10, .ttl_secs = 10},
                }});

  response = exec(dir,
                  state,
                  t + 11,
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id_2,
                            .request_no = request_no++,
                            .commands   = {
                                NodeExistsCommand{.path = "/test/123"},
                                NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                            }});

  response = exec(dir,
                  state,
                  t + 13,
                  RaftIndex{++log_idx},
                  ZkRequest{.session_id = session_id_2,
                            .request_no = request_no++,
                            .commands   = {
                                NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                            }});
}

