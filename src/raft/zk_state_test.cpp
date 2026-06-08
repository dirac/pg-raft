#define BOOST_TEST_MODULE zk_state
#include "lib/servers/zookeeper/zk_state.hpp"

#include "lib/base/testing.hpp"

namespace banyandb::servers::zookeeper::test {

using namespace banyandb::base;

namespace {
ZkState clone(ZkState const& x) {
  BOOST_TEST(x.copy() == x);
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
}  // namespace

BOOST_AUTO_TEST_CASE(create_node_seq) {
  ZkState state;

  BOOST_TEST((state == clone(state)));

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST(state == clone(state));

  BOOST_TEST((response.state == SessionState::kConnected));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  BOOST_TEST(response.events.size() == 0);
  BOOST_TEST(response.results.size() == 1);
  BOOST_TEST(response.zxid == Zxid{log_idx});

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;
  BOOST_TEST(session_id > 0);

  // Create a node "/top".
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {CreateNodeCommand{.path = "/top", .value = "test"}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST(state.strings() == set<string>{"top"});

  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path == "/top");

  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).stat.mzxid == Zxid{log_idx});
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).stat.czxid == Zxid{log_idx});

  // Create a sequential node "/top", should result in a node "/top0000000000000000001"
  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {CreateNodeCommand{
                                        .path = "/top", .value = "test", .sequential = true}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST(state.strings() == set<string>{"top"});

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path ==
             "/top0000000000000000001");

  // The sequence number is less than ZkState::kSequenceThreshold. Therefore we can always represent
  // in 19 digits with at least one '0' for most significant digit.
  size_t seq_length = std::get<CreateNodeResult>(response.results.at(0).result).path.size() - 4U;
  BOOST_TEST(seq_length == std::strlen("0000000000000000001"));
  BOOST_TEST(seq_length == std::to_string(ZkState::kSequenceThreshold).size());

  // The root node's sequence is 1 so 2 should be created.
  // Create a sequential node "/t", should result in a node "/t0000000000000000002"
  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {CreateNodeCommand{
                                        .path = "/t", .value = "test", .sequential = true}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST((state.strings() == set<string>{"t", "top"}));

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path ==
             "/t0000000000000000002");

  // Creating a node with zero length name is invalid.
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {CreateNodeCommand{.path = "/top/", .value = "test"}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kInvalidNodePath));
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kInvalidNodePath));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {CreateNodeCommand{
                                        .path = "/top/", .value = "test", .sequential = true}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kInvalidNodePath));
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kInvalidNodePath));

  // Create a sequential node "/top/t", should create "/top/t0000000000000000001"
  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {CreateNodeCommand{
                                        .path = "/top/t", .value = "test", .sequential = true}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST((state.strings() == set<string>{"top", "t"}));

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path ==
             "/top/t0000000000000000001");

  // Create a sequential node "/t0000000000000000002", where "/t0000000000000000002" is an existing
  // sequential node.
  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {CreateNodeCommand{
                      .path = "/t0000000000000000002", .value = "test", .sequential = true}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST((state.strings() == set<string>{"t", "t0000000000000000002", "top"}));

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path ==
             "/t00000000000000000020000000000000000003");

  // Create a child node to a sequential node "/t0000000000000000002".
  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {CreateNodeCommand{
                      .path = "/t0000000000000000002/t", .value = "test", .sequential = true}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST((state.strings() == set<string>{"t", "t0000000000000000002", "top"}));

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path ==
             "/t0000000000000000002/t0000000000000000001");

  // Create another sequential child node to a sequential node "/t0000000000000000002", with a
  // prefix "/t-".
  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {CreateNodeCommand{
                      .path = "/t0000000000000000002/t-", .value = "test", .sequential = true}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST((state.strings() == set<string>{"t", "t-", "t0000000000000000002", "top"}));

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path ==
             "/t0000000000000000002/t-0000000000000000002");

  // Create a sub node to a sequential node "/t0000000000000000002/t-".
  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {CreateNodeCommand{
                      .path = "/t0000000000000000002/t-", .value = "test", .sequential = false}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST((state.strings() == set<string>{"t", "t-", "t0000000000000000002", "top"}));

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path ==
             "/t0000000000000000002/t-");

  // Create a child node to a sequential node "/top0000000000000000001/child"
  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{
          .session_id = session_id,
          .request_no = request_no++,
          .commands   = {CreateNodeCommand{
                .path = "/top0000000000000000001/child", .value = "test", .sequential = false}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST((state.strings() == set<string>{"child", "t", "t-", "t0000000000000000002", "top"}));

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path ==
             "/top0000000000000000001/child");

  // Create a child node to a non-sequential node "/top/child"
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {CreateNodeCommand{
                                 .path = "/top/child", .value = "test", .sequential = false}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST((state.strings() == set<string>{"child", "t", "t-", "t0000000000000000002", "top"}));

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path == "/top/child");
}

BOOST_AUTO_TEST_CASE(create_node_numbers) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;
  BOOST_TEST(session_id > 0);

  // Create a node "/top".
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {CreateNodeCommand{.path = "/top", .value = "test"}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST(state.strings() == set<string>{"top"});

  // Create a node "/top/<kSequenceThreshold - 1>".
  string n1 = fn::format("/top/{}", ZkState::kSequenceThreshold - 1U);
  response  = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                   .request_no = request_no++,
                                   .commands   = {CreateNodeCommand{.path = n1, .value = "test"}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path == n1);
  BOOST_TEST(state.strings() == set<string>{"top"});

  string n2 = fn::format("/top/{}", ZkState::kSequenceThreshold);
  response  = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                   .request_no = request_no++,
                                   .commands   = {CreateNodeCommand{.path = n2, .value = "test"}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path == n2);

  BOOST_TEST(state.strings() == (set<string>{"top", fn::to_string(ZkState::kSequenceThreshold)}));

  string c1 = fn::format("{0}/{1}", n1, ZkState::kSequenceThreshold);
  response  = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                   .request_no = request_no++,
                                   .commands   = {CreateNodeCommand{.path = c1, .value = "test"}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path == c1);

  string c2 = fn::format("{0}/{1}", n2, ZkState::kSequenceThreshold - 1U);
  response  = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                   .request_no = request_no++,
                                   .commands   = {CreateNodeCommand{.path = c2, .value = "test"}}});
  BOOST_TEST(state == clone(state));
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<CreateNodeResult>(response.results.at(0).result).path == c2);

  BOOST_TEST(state.strings() == (set<string>{"top", fn::to_string(ZkState::kSequenceThreshold)}));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {SetValueCommand{.path = c1, .value = "test2"}}});
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {SetValueCommand{.path = c2, .value = "test2"}}});
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
}

BOOST_AUTO_TEST_CASE(create_node_failures) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST((response.state == SessionState::kConnected));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  // Empty path
  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {CreateNodeCommand{.path = "", .value = "test"}}});
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kInvalidNodePath));

  // path not starting with '/'
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {CreateNodeCommand{.path = "test", .value = "test"}}});
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kInvalidNodePath));

  // value having some special chars
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {CreateNodeCommand{.path = "/test", .value = "te\nst"}}});
  BOOST_TEST(to_string(response.results.at(0).status) ==
             to_string(ResultStatus::kInvalidNodeValue));

  // No parent
  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {CreateNodeCommand{.path = "/test/child", .value = "test"}}});
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kNoNode));

  // Parent can't be ephemeral
  {
    response =
        state.exec(std::time(nullptr),
                   RaftIndex{++log_idx},
                   ZkRequest{.session_id = session_id,
                             .request_no = request_no++,
                             .commands   = {CreateNodeCommand{
                                   .path = "/ephemeral", .value = "test", .ephemeral = true}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands   = {CreateNodeCommand{.path = "/ephemeral/child", .value = "test"}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kNodeEphemeral));
  }

  // Parent can't be frozen
  {
    response =
        state.exec(std::time(nullptr),
                   RaftIndex{++log_idx},
                   ZkRequest{.session_id = session_id,
                             .request_no = request_no++,
                             .commands = {CreateNodeCommand{.path = "/frozen", .value = "test"}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    response =
        state.exec(std::time(nullptr),
                   RaftIndex{++log_idx},
                   ZkRequest{.session_id = session_id,
                             .request_no = request_no++,
                             .commands   = {EraseNodeCommand{.path = "/frozen", .ttl_secs = 200}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands   = {CreateNodeCommand{.path = "/frozen/child", .value = "test"}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kNodeFrozen));
  }

  // Length of any segment should be less than or equal to ZkState::kSegmentSizeLimit (64)
  {
    string seg{"/"};
    while (seg.size() <= ZkState::kSegmentSizeLimit)
      seg += 'a';

    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id,
                                    .request_no = request_no++,
                                    .commands = {CreateNodeCommand{.path = seg, .value = "test"}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{
            .session_id = session_id,
            .request_no = request_no++,
            .commands = {CreateNodeCommand{.path = "/top", .value = "test", .if_not_exists = true},
                         CreateNodeCommand{.path = string{"/top"} + seg, .value = "test"}}});

    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
    BOOST_TEST(to_string(response.results.at(1).status) == to_string(ResultStatus::kOk));

    seg += 'b';
    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id,
                                    .request_no = request_no++,
                                    .commands = {CreateNodeCommand{.path = seg, .value = "test"}}});
    BOOST_TEST(to_string(response.results.at(0).status) ==
               to_string(ResultStatus::kInvalidNodePath));

    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands = {CreateNodeCommand{.path = string{"/top"} + seg, .value = "test"}}});
    BOOST_TEST(to_string(response.results.at(0).status) ==
               to_string(ResultStatus::kInvalidNodePath));
  }

  // Length of any segment should be less than or equal to ZkState::kSegmentSizeLimit (64)
  // so for sequential the prefix limit is ZkState::kSegmentSizeLimit - ZkState::kSequenceDigits.
  {
    string seg{"/"};
    while (seg.size() <= (ZkState::kSegmentSizeLimit - ZkState::kSequenceDigits))
      seg += 'a';

    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id,
                                    .request_no = request_no++,
                                    .commands   = {CreateNodeCommand{
                                          .path = seg, .value = "test", .sequential = true}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands   = {CreateNodeCommand{
                        .path = string{"/top"} + seg, .value = "test", .sequential = true}}});

    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    seg += 'b';
    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id,
                                    .request_no = request_no++,
                                    .commands   = {CreateNodeCommand{
                                          .path = seg, .value = "test", .sequential = true}}});
    BOOST_TEST(to_string(response.results.at(0).status) ==
               to_string(ResultStatus::kInvalidNodePath));

    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands   = {CreateNodeCommand{
                        .path = string{"/top"} + seg, .value = "test", .sequential = true}}});
    BOOST_TEST(to_string(response.results.at(0).status) ==
               to_string(ResultStatus::kInvalidNodePath));
  }

  // Node exits.
  {
    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands   = {CreateNodeCommand{.path = "/node_exists", .value = "test"}}});

    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands   = {CreateNodeCommand{
                        .path = "/node_exists/seq", .value = "test", .sequential = true}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    response =
        state.exec(std::time(nullptr),
                   RaftIndex{++log_idx},
                   ZkRequest{.session_id = session_id,
                             .request_no = request_no++,
                             .commands   = {CreateNodeCommand{
                                   .path = "/node_exists/seq0000000000000000001", .value = "test"}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kNodeExists));

    response =
        state.exec(std::time(nullptr),
                   RaftIndex{++log_idx},
                   ZkRequest{.session_id = session_id,
                             .request_no = request_no++,
                             .commands   = {CreateNodeCommand{
                                   .path = "/node_exists/seq0000000000000000002", .value = "test"}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id,
                  .request_no = request_no++,
                  .commands   = {CreateNodeCommand{
                        .path = "/node_exists/seq", .value = "test", .sequential = true}}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kNodeExists));

    // .if_not_exists
    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id,
                                    .request_no = request_no++,
                                    .commands   = {CreateNodeCommand{
                                          .path          = "/node_exists/seq0000000000000000001",
                                          .value         = "test",
                                          .if_not_exists = true,
                                    }}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    // On success should the counter be incremented ?
    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id,
                                    .request_no = request_no++,
                                    .commands   = {CreateNodeCommand{
                                          .path          = "/node_exists/seq",
                                          .value         = "test",
                                          .sequential    = true,
                                          .if_not_exists = true,
                                    }}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    // On success should the counter be incremented ? Assumption is NO.
    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id,
                                    .request_no = request_no++,
                                    .commands   = {CreateNodeCommand{
                                          .path       = "/node_exists/seq",
                                          .value      = "test",
                                          .sequential = true,
                                    }}});
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kNodeExists));
  }
}

BOOST_AUTO_TEST_CASE(new_node_rollback) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST((response.state == SessionState::kConnected));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{
          .session_id = session_id,
          .request_no = request_no++,
          .commands   = {CreateNodeCommand{.path = "/test", .value = "test"},
                         CreateNodeCommand{.path = "/test/seq", .value = "test", .sequential = true},
                         CreateNodeCommand{.path = "/test/child", .value = "test"},
                         CreateNodeCommand{.path = "/test/child/gchild", .value = "test"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  // Create a new session.
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST((response.state == SessionState::kConnected));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

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

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{
                            .session_id = session_id,
                            .request_no = request_no++,
                            .commands   = commands,
                        });

  BOOST_TEST(state.verify_rollback(session_id, s));

  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));
  for (size_t i = 0; i < commands.size() - 1U; ++i) {
    BOOST_TEST(to_string(response.results.at(i).status) == to_string(ResultStatus::kOk));
  }
  BOOST_TEST(to_string(response.results.at(commands.size() - 1U).status) ==
             to_string(ResultStatus::kNoNode));
}

BOOST_AUTO_TEST_CASE(create_node_stat) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST((response.state == SessionState::kConnected));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
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
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  auto zxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  auto test_stat        = std::get<NodeExistsResult>(response.results.at(0).result).stat;
  auto test_seq_stat    = std::get<NodeExistsResult>(response.results.at(1).result).stat;
  auto test_123_stat    = std::get<NodeExistsResult>(response.results.at(2).result).stat;
  auto test_child_stat  = std::get<NodeExistsResult>(response.results.at(3).result).stat;
  auto test_gchild_stat = std::get<NodeExistsResult>(response.results.at(4).result).stat;

  BOOST_TEST(test_stat.mzxid == zxid);
  BOOST_TEST(test_stat.czxid == zxid);

  BOOST_TEST(test_seq_stat.mzxid == zxid);
  BOOST_TEST(test_seq_stat.czxid == zxid);

  BOOST_TEST(test_123_stat.mzxid == zxid);
  BOOST_TEST(test_123_stat.czxid == zxid);

  BOOST_TEST(test_child_stat.mzxid == zxid);
  BOOST_TEST(test_child_stat.czxid == zxid);

  BOOST_TEST(test_gchild_stat.mzxid == zxid);
  BOOST_TEST(test_gchild_stat.czxid == zxid);

  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{
          .session_id = session_id,
          .request_no = request_no++,
          .commands   = {
              CreateNodeCommand{.path = "/test/seq0000000000000000001/child", .value = "test"},
          }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_seq_stat.czxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));
  BOOST_TEST((test_child_stat == std::get<NodeExistsResult>(response.results.at(3).result).stat));
  BOOST_TEST((test_gchild_stat == std::get<NodeExistsResult>(response.results.at(4).result).stat));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {
                                      CreateNodeCommand{.path = "/test/123/child", .value = "test"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_123_stat.czxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));
  BOOST_TEST((test_child_stat == std::get<NodeExistsResult>(response.results.at(3).result).stat));
  BOOST_TEST((test_gchild_stat == std::get<NodeExistsResult>(response.results.at(4).result).stat));

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {
                               CreateNodeCommand{.path = "/test/child/child2", .value = "test"},
                           }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_child_stat.czxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));
  BOOST_TEST((test_child_stat == std::get<NodeExistsResult>(response.results.at(3).result).stat));
  BOOST_TEST((test_gchild_stat == std::get<NodeExistsResult>(response.results.at(4).result).stat));

  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {
                    CreateNodeCommand{.path = "/test/child/gchild/child", .value = "test"},
                }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_gchild_stat.czxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));
  BOOST_TEST((test_child_stat == std::get<NodeExistsResult>(response.results.at(3).result).stat));
  BOOST_TEST((test_gchild_stat == std::get<NodeExistsResult>(response.results.at(4).result).stat));
}

BOOST_AUTO_TEST_CASE(set_value_stat) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST((response.state == SessionState::kConnected));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
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
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  auto zxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  auto test_stat        = std::get<NodeExistsResult>(response.results.at(0).result).stat;
  auto test_seq_stat    = std::get<NodeExistsResult>(response.results.at(1).result).stat;
  auto test_123_stat    = std::get<NodeExistsResult>(response.results.at(2).result).stat;
  auto test_child_stat  = std::get<NodeExistsResult>(response.results.at(3).result).stat;
  auto test_gchild_stat = std::get<NodeExistsResult>(response.results.at(4).result).stat;

  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {
                    SetValueCommand{.path = "/test/seq0000000000000000001", .value = "test"},
                }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_seq_stat.mzxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));
  BOOST_TEST((test_child_stat == std::get<NodeExistsResult>(response.results.at(3).result).stat));
  BOOST_TEST((test_gchild_stat == std::get<NodeExistsResult>(response.results.at(4).result).stat));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {
                                      SetValueCommand{.path = "/test/123", .value = "test"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_123_stat.mzxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));
  BOOST_TEST((test_child_stat == std::get<NodeExistsResult>(response.results.at(3).result).stat));
  BOOST_TEST((test_gchild_stat == std::get<NodeExistsResult>(response.results.at(4).result).stat));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {
                                      SetValueCommand{.path = "/test/child", .value = "test"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_child_stat.mzxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));
  BOOST_TEST((test_child_stat == std::get<NodeExistsResult>(response.results.at(3).result).stat));
  BOOST_TEST((test_gchild_stat == std::get<NodeExistsResult>(response.results.at(4).result).stat));

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {
                               SetValueCommand{.path = "/test/child/gchild", .value = "test"},
                           }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_gchild_stat.mzxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));
  BOOST_TEST((test_child_stat == std::get<NodeExistsResult>(response.results.at(3).result).stat));
  BOOST_TEST((test_gchild_stat == std::get<NodeExistsResult>(response.results.at(4).result).stat));
}

BOOST_AUTO_TEST_CASE(erase_node_stat) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST((response.state == SessionState::kConnected));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
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
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  auto zxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"},
                                          NodeExistsCommand{.path = "/test/child/gchild"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  auto test_stat       = std::get<NodeExistsResult>(response.results.at(0).result).stat;
  auto test_seq_stat   = std::get<NodeExistsResult>(response.results.at(1).result).stat;
  auto test_123_stat   = std::get<NodeExistsResult>(response.results.at(2).result).stat;
  auto test_child_stat = std::get<NodeExistsResult>(response.results.at(3).result).stat;

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {
                                      EraseNodeCommand{.path = "/test/child/gchild"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_child_stat.czxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"},
                                          NodeExistsCommand{.path = "/test/child"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));
  BOOST_TEST((test_child_stat == std::get<NodeExistsResult>(response.results.at(3).result).stat));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {
                                      EraseNodeCommand{.path = "/test/child"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_stat.czxid = Zxid{log_idx};

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {NodeExistsCommand{.path = "/test"},
                                          NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/test/123"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));
  BOOST_TEST((test_123_stat == std::get<NodeExistsResult>(response.results.at(2).result).stat));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {
                                      EraseNodeCommand{.path = "/test/123"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_stat.czxid = Zxid{log_idx};

  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id,
                .request_no = request_no++,
                .commands   = {NodeExistsCommand{.path = "/test"},
                               NodeExistsCommand{.path = "/test/seq0000000000000000001"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
  BOOST_TEST((test_seq_stat == std::get<NodeExistsResult>(response.results.at(1).result).stat));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {
                                      EraseNodeCommand{.path = "/test/seq0000000000000000001"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  test_stat.czxid = Zxid{log_idx};

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {NodeExistsCommand{.path = "/test"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST((test_stat == std::get<NodeExistsResult>(response.results.at(0).result).stat));
}

BOOST_AUTO_TEST_CASE(erase_failures) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  int64_t  request_no = 1;
  uint64_t log_idx    = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST((response.state == SessionState::kConnected));
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
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
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {EraseNodeCommand{.path = ""}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kInvalidNodePath));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {EraseNodeCommand{.path = "/"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kInvalidNodePath));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {EraseNodeCommand{.path = "/test/123/"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kInvalidNodePath));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {EraseNodeCommand{.path = "/test/12"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands = {EraseNodeCommand{.path = "/test/123", .zxid = Zxid{42}}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kBadVersion));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {EraseNodeCommand{.path = "/test/child"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kHasChildren));

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {EraseNodeCommand{.path = "/test/123", .ttl_secs = 60}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {NodeExistsCommand{.path = "/test/123"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {EraseNodeCommand{.path = "/test/123"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNodeFrozen));
}

BOOST_AUTO_TEST_CASE(erase_success) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  uint64_t log_idx = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = 0,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id_1 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
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
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  // Create another session.
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = 0,
                           .commands = {NewSessionCommand{.name = "test2", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id_2 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id_2,
                .request_no = 1,
                .commands   = {GetValueCommand{.path = "/test/123", .watch = true},
                               GetChildrenCommand{.path = "/test/child/gchild", .watch = true}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  // Create another session.
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = 0,
                           .commands = {NewSessionCommand{.name = "test3", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id_3 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  {
    // Erase /test/seq0000000000000000001, updates ephemeral list
    BOOST_TEST(state.sessions()[session_id_1].ephemeral.contains("/test/seq0000000000000000001"));
    response = state.exec(
        std::time(nullptr),
        RaftIndex{++log_idx},
        ZkRequest{.session_id = session_id_3,
                  .request_no = 1,
                  .commands   = {EraseNodeCommand{.path = "/test/seq0000000000000000001"}}});
    BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
    BOOST_TEST(!state.sessions()[session_id_1].ephemeral.contains("/test/seq0000000000000000001"));
  }

  {
    // Erase /test/123 update deletes list
    BOOST_TEST(state.sessions()[session_id_1].deletes.size() == 0);
    BOOST_TEST(state.sessions()[session_id_2].deletes.size() == 0);
    BOOST_TEST(state.sessions()[session_id_3].deletes.size() == 0);

    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id_3,
                                    .request_no = 2,
                                    .commands   = {EraseNodeCommand{.path = "/test/123"}}});
    BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

    BOOST_TEST(state.sessions()[session_id_1].deletes.size() == 0);
    BOOST_TEST(state.sessions()[session_id_2].deletes == set<string>{"/test/123"});
    BOOST_TEST(state.sessions()[session_id_3].deletes.size() == 0);
  }

  {
    // Erase /test/child/gchild update deletes list
    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id_3,
                                    .request_no = 3,
                                    .commands = {EraseNodeCommand{.path = "/test/child/gchild"}}});
    BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

    BOOST_TEST(state.sessions()[session_id_1].deletes.size() == 0);
    BOOST_TEST(state.sessions()[session_id_2].deletes ==
               (set<string>{"/test/123", "/test/child/gchild"}));
    BOOST_TEST(state.sessions()[session_id_3].deletes.size() == 0);
  }

  {
    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id_3,
                                    .request_no = 4,
                                    .commands   = {EraseNodeCommand{.path = "/test/child"},
                                                   EraseNodeCommand{.path = "/test"}}});
    BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  }

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id_2,
                                  .request_no = 2,
                                  .commands   = {GetValueCommand{.path = "/test"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));

  BOOST_TEST((response.events ==
              set<WatchEvent>{
                  WatchEvent{.path = "/test/123", .type = WatchEventType::kNodeEvent},
                  WatchEvent{.path = "/test/child/gchild", .type = WatchEventType::kChildEvent}}));
}

BOOST_AUTO_TEST_CASE(erase_rollback) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  uint64_t log_idx = INT(state.curr_zxid());

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = 0,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id_1 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
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
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  // Create another session.
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = 0,
                           .commands = {NewSessionCommand{.name = "test2", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id_2 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
      std::time(nullptr),
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id_2,
                .request_no = 1,
                .commands   = {GetValueCommand{.path = "/test/123", .watch = true},
                               GetChildrenCommand{.path = "/test/child/gchild", .watch = true}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  // Create another session.
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = 0,
                           .commands = {NewSessionCommand{.name = "test3", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id_3 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  {
    ZkState s = clone(state);

    BOOST_TEST(state.sessions()[session_id_1].ephemeral.contains("/test/seq0000000000000000001"));
    response =
        state.exec(std::time(nullptr),
                   RaftIndex{++log_idx},
                   ZkRequest{.session_id = session_id_3,
                             .request_no = 1,
                             .commands = {EraseNodeCommand{.path = "/test/seq0000000000000000001"},
                                          NodeExistsCommand{.path = "/non-existent"}}});
    BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    BOOST_TEST(state.verify_rollback(session_id_3, s));
  }

  {
    ZkState s = clone(state);
    // Erase /test/123 update deletes list
    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id_3,
                                    .request_no = 2,
                                    .commands   = {EraseNodeCommand{.path = "/test/123"},
                                                   NodeExistsCommand{.path = "/non-existent"}}});
    BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    BOOST_TEST(state.verify_rollback(session_id_3, s));
  }

  {
    ZkState s = clone(state);

    response = state.exec(std::time(nullptr),
                          RaftIndex{++log_idx},
                          ZkRequest{.session_id = session_id_3,
                                    .request_no = 3,
                                    .commands   = {EraseNodeCommand{.path = "/test/child/gchild"},
                                                   NodeExistsCommand{.path = "/non-existent"}}});
    BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));
    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));

    BOOST_TEST(state.verify_rollback(session_id_3, s));
  }

  {
    ZkState s = clone(state);

    response =
        state.exec(std::time(nullptr),
                   RaftIndex{++log_idx},
                   ZkRequest{.session_id = session_id_3,
                             .request_no = 4,
                             .commands   = {EraseNodeCommand{.path = "/test/child/gchild"},
                                            EraseNodeCommand{.path = "/test/child"},
                                            EraseNodeCommand{.path = "/test/123"},
                                            EraseNodeCommand{.path = "/test/seq0000000000000000001"},
                                            EraseNodeCommand{.path = "/test"},
                                            NodeExistsCommand{.path = "/non-existent"}}});
    BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));

    BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
    BOOST_TEST(to_string(response.results.at(1).status) == to_string(ResultStatus::kOk));
    BOOST_TEST(to_string(response.results.at(2).status) == to_string(ResultStatus::kOk));
    BOOST_TEST(to_string(response.results.at(3).status) == to_string(ResultStatus::kOk));
    BOOST_TEST(to_string(response.results.at(4).status) == to_string(ResultStatus::kOk));

    BOOST_TEST(state.verify_rollback(session_id_3, s));
  }
  {
    response = state.exec(std::time(nullptr),
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
    BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  }
}

BOOST_AUTO_TEST_CASE(rollback) {
  ZkState state;

  uint64_t log_idx    = INT(state.curr_zxid());
  int64_t  request_no = 0;

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  auto s = clone(state);

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands = {CreateNodeCommand{.path = "/test", .value = "test"},
                                               SetValueCommand{.path = "/test", .value = "test2"},
                                               GetValueCommand{.path = "/test", .watch = true},
                                               GetChildrenCommand{.path = "/test", .watch = true},
                                               EraseNodeCommand{.path = "/test"},
                                               NodeExistsCommand{.path = "/non-existent"}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));

  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(to_string(response.results.at(1).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(to_string(response.results.at(2).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(to_string(response.results.at(3).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(to_string(response.results.at(4).status) == to_string(ResultStatus::kOk));

  BOOST_TEST(state.verify_rollback(session_id, s));
}

BOOST_AUTO_TEST_CASE(get_ops) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  uint64_t log_idx    = INT(state.curr_zxid());
  int64_t  request_no = 0;

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id = std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
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
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {GetValueCommand{.path = "/test"},
                                                 GetChildrenCommand{.path = "/test"}}});

  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST(std::get<GetValueResult>(response.results.at(0).result).value == "test");
  BOOST_TEST(std::get<GetChildrenResult>(response.results.at(1).result).children ==
             (vector<string>{"123", "child", "seq0000000000000000001"}));

  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = session_id,
                           .request_no = request_no++,
                           .commands   = {
                               SetValueCommand{.path = "/test", .value = "test2"},
                               EraseNodeCommand{.path = "/test/123"},
                               CreateNodeCommand{.path = "/test/apple", .value = "x"},
                               CreateNodeCommand{.path = "/test/child/gchild2", .value = "x"},
                           }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id,
                                  .request_no = request_no++,
                                  .commands   = {GetValueCommand{.path = "/test"},
                                                 GetChildrenCommand{.path = "/test"}}});

  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  BOOST_TEST(std::get<GetValueResult>(response.results.at(0).result).value == "test2");
  BOOST_TEST(std::get<GetChildrenResult>(response.results.at(1).result).children ==
             (vector<string>{"apple", "child", "seq0000000000000000001"}));
}

BOOST_AUTO_TEST_CASE(close_session) {
  ZkState state;

  BOOST_TEST(state == clone(state));

  uint64_t log_idx    = INT(state.curr_zxid());
  int64_t  request_no = 0;

  // Create a session.
  ZkResponse response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id_1 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(
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
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  // Create another session.
  response =
      state.exec(std::time(nullptr),
                 RaftIndex{++log_idx},
                 ZkRequest{.session_id = 0,
                           .request_no = request_no++,
                           .commands = {NewSessionCommand{.name = "test2", .timeout_seconds = 5}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  uint64_t const session_id_2 =
      std::get<NewSessionResult>(response.results.at(0).result).session_id;

  response = state.exec(std::time(nullptr),
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id_2,
                                  .request_no = request_no++,
                                  .commands   = {
                                      NodeExistsCommand{.path = "/test/123"},
                                      NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                      NodeExistsCommand{.path = "/test/child/gchild"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  auto t = std::time(nullptr);

  response = state.exec(
      t,
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id_2,
                .request_no = request_no++,
                .commands   = {
                    EraseNodeCommand{.path = "/test/seq0000000000000000001", .ttl_secs = 10},
                }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));
  BOOST_TEST(std::get<EraseNodeResult>(response.results.at(0).result).expiry.value() == t + 10);

  response = state.exec(t,
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id_2,
                                  .request_no = request_no++,
                                  .commands   = {
                                      NodeExistsCommand{.path = "/test/123"},
                                      NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                      NodeExistsCommand{.path = "/test/child/gchild"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  response = state.exec(t,
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = 0,
                                  .request_no = 0,
                                  .commands   = {CloseSessionCommand{.session_id = session_id_1}}});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  response = state.exec(t + 2,
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id_2,
                                  .request_no = request_no++,
                                  .commands   = {
                                      NodeExistsCommand{.path = "/test/123"},
                                      NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                      NodeExistsCommand{.path = "/test/child/gchild"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));
  BOOST_TEST(to_string(response.results.at(0).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(to_string(response.results.at(1).status) == to_string(ResultStatus::kOk));
  BOOST_TEST(to_string(response.results.at(2).status) == to_string(ResultStatus::kNoNode));

  response = state.exec(
      t + 3,
      RaftIndex{++log_idx},
      ZkRequest{.session_id = session_id_2,
                .request_no = request_no++,
                .commands   = {
                    ExtendTTLCommand{
                          .path = "/test/seq0000000000000000001", .expiry = t + 10, .ttl_secs = 10},
                }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  response = state.exec(t + 11,
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id_2,
                                  .request_no = request_no++,
                                  .commands   = {
                                      NodeExistsCommand{.path = "/test/123"},
                                      NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kOk));

  response = state.exec(t + 13,
                        RaftIndex{++log_idx},
                        ZkRequest{.session_id = session_id_2,
                                  .request_no = request_no++,
                                  .commands   = {
                                      NodeExistsCommand{.path = "/test/seq0000000000000000001"},
                                  }});
  BOOST_TEST(to_string(response.status) == to_string(ResultStatus::kNoNode));
}

}  // namespace banyandb::servers::zookeeper::test
