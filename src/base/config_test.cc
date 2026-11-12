#define BOOST_TEST_MODULE config
#include "src/base/config.h"

#include <fstream>

#include "src/base/testing.h"

// Config parsing: the servers, pg_config, and the node's settings with their defaults.

BOOST_AUTO_TEST_CASE(node_settings) {
  TmpDir tmp;
  auto   file = tmp() / "config.toml";
  std::ofstream(file) << R"(
dir = "/var/pg_raft"
backup_store = "/backups/cluster1"
backup_interval_secs = 18000
backup_wal_size = 5368709120
backup_pending_timeout_secs = 60
socket_dir = "/tmp/pgr"

[[server]]
hostname = "a"
pg_port  = 5433
ha_port  = 7000

[[server]]
hostname = "b"
pg_port  = 5434
ha_port  = 7001

[pg_config]
max_connections = 100
)";

  auto c = Config{file.string()};
  BOOST_TEST(c.wdir == "/var/pg_raft");
  BOOST_TEST(c.backup_store == "/backups/cluster1");
  BOOST_TEST(c.backup_interval_secs == 18000U);
  BOOST_TEST(c.backup_wal_size == 5368709120ULL);
  BOOST_TEST(c.backup_pending_timeout_secs == 60U);
  BOOST_TEST(c.socket_dir == "/tmp/pgr");
  BOOST_REQUIRE(c.servers.size() == 2U);
  BOOST_TEST(c.servers.at(1).hostname == "b");
  BOOST_TEST(c.servers.at(1).ha_port == 7001);
  BOOST_TEST(c.pg_config.at("max_connections") == "100");
}

BOOST_AUTO_TEST_CASE(node_defaults) {
  TmpDir tmp;
  auto   file = tmp() / "config.toml";
  std::ofstream(file) << "dir = \"/x\"\n";

  auto c = Config{file.string()};
  BOOST_TEST(c.backup_store.empty());
  BOOST_TEST(c.backup_interval_secs == 6U * 3600U);
  BOOST_TEST(c.backup_wal_size == uint64_t{4} << 30U);
  BOOST_TEST(c.backup_pending_timeout_secs == 120U);
  BOOST_TEST(c.socket_dir == "/tmp");
}
