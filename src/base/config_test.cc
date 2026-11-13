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
leader_sync = true
follower_sync = true
log_sync = true
write_op_bytes = 131072
frame_checksums = true
pg_hba = ["host all all 10.0.0.0/16 trust", "host replication all 10.0.0.0/16 trust"]
proxy_pg_port = 6432
proxy_http_port = 6480

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
  BOOST_TEST(c.leader_sync);
  BOOST_TEST(c.follower_sync);
  BOOST_TEST(c.log_sync);
  BOOST_TEST(c.write_op_bytes == 131072U);
  BOOST_TEST(c.frame_checksums);
  BOOST_REQUIRE(c.pg_hba.size() == 2U);
  BOOST_TEST(c.pg_hba.at(1) == "host replication all 10.0.0.0/16 trust");
  BOOST_TEST(c.proxy_pg_port == 6432);
  BOOST_TEST(c.proxy_http_port == 6480);
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
  // No disk syncs on the commit path unless asked: durability by replication.
  BOOST_TEST(!c.leader_sync);
  BOOST_TEST(!c.follower_sync);
  BOOST_TEST(!c.log_sync);
  BOOST_TEST(c.write_op_bytes == 64U * 1024U);
  BOOST_TEST(!c.frame_checksums);
  BOOST_TEST(c.pg_hba.empty());
  BOOST_TEST(c.proxy_pg_port == 5432);
  BOOST_TEST(c.proxy_http_port == 8080);
}
