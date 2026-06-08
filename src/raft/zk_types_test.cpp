#define BOOST_TEST_MODULE zk_types
#include "lib/servers/zookeeper/zk_types.hpp"

#include "lib/base/testing.hpp"

namespace banyandb::servers::zookeeper::test {

using namespace banyandb::base;

namespace {
template <class X>
X clone(X const& x) {
  return std::move(from_encoded_bytes<X>(to_encoded_bytes(x)).value());
}

Stat stat() {
  return Stat{
      .mzxid = Zxid{42},
      .czxid = Zxid{52},
  };
}

}  // namespace

BOOST_AUTO_TEST_CASE(stat_test) {
  Stat c{
      .mzxid = Zxid{42},
      .czxid = Zxid{52},
  };

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(newsessioncommand) {
  NewSessionCommand c{
      .name            = "test",
      .timeout_seconds = 42,
  };

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(newsessionresult) {
  NewSessionResult c{.session_id = 42};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(closesessioncommand) {
  CloseSessionCommand c{.session_id = 42};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(closesessionresult) {
  CloseSessionResult c{};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(getvaluecommand) {
  GetValueCommand c{.path = "/test", .watch = false, .quorum_read = true};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(getvalueresult) {
  GetValueResult c{.value = "test", .stat = stat()};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(getchildrencommand) {
  GetChildrenCommand c{.path = "/test", .watch = false, .quorum_read = true};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(getchildrenresult) {
  GetChildrenResult c{.path = "/test", .children = {"c1"}, .stat = stat()};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(nodeexistscommand) {
  NodeExistsCommand c{.path = "/test", .zxid = Zxid{42}};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(nodeexistsresult) {
  NodeExistsResult c{.stat = stat()};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(setvaluecommand) {
  SetValueCommand c{.path = "/test", .value = "test", .zxid = Zxid{42}};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(setvalueresult) {
  SetValueResult c{.stat = stat()};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(createnodecommand) {
  CreateNodeCommand c{.path          = "/test",
                      .value         = "test",
                      .ephemeral     = true,
                      .sequential    = true,
                      .if_not_exists = true};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(createnoderesult) {
  CreateNodeResult c{.path = "/test", .stat = stat()};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(erasenodecommand) {
  EraseNodeCommand c{.path = "/test", .zxid = Zxid{42}, .ttl_secs = 42};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(erasenoderesult) {
  EraseNodeResult c{.expiry = 443232};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(extendttlcommand) {
  ExtendTTLCommand c{.path = "/test", .expiry = 42, .ttl_secs = 32};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(extendttlresult) {
  ExtendTTLResult c{.expiry = 4343};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(commandresult) {
  CommandResult c{.status = ResultStatus::kNoNode, .result = ExtendTTLResult{.expiry = 322}};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(zkrequest) {
  ZkRequest c{.session_id = 42,
              .request_no = 545,
              .commands   = {ExtendTTLCommand{.path = "/test", .expiry = 42, .ttl_secs = 32}}};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(watchevent) {
  WatchEvent c{.path = "/test", .type = WatchEventType::kChildEvent};

  BOOST_TEST((c == clone(c)));
}
BOOST_AUTO_TEST_CASE(zkresponse) {
  ZkResponse c{.state   = SessionState::kExpired,
               .status  = ResultStatus::kNoNode,
               .events  = {WatchEvent{.path = "/test", .type = WatchEventType::kChildEvent}},
               .results = {CommandResult{.status = ResultStatus::kNoNode,
                                         .result = ExtendTTLResult{.expiry = 322}}}};

  BOOST_TEST((c == clone(c)));
}

}  // namespace banyandb::servers::zookeeper::test
