#pragma once

#include "src/base/base.h"
#include "src/base/logger.h"

///
/// The configuration used by the system. The configuration is specified in
/// [toml](https://toml.io/en/)
///
struct Config {
  /// The temp, data and raft directories will reside as subdirs.
  string wdir;

  /// All the postgresql installations. Current and next.
  string install_dir = "/opt/pg_raft/postgresql";

  /// The desired pg_version. If the data directory belongs to a previous version, then the leader
  /// is going to upgrade.
  unsigned pg_major_version = 17;

  struct Server {
    string hostname;

    uint16_t pg_port{0};  ///< postgresql port.
    uint16_t ha_port{0};  ///< Raft Port.
  };
  vector<Server> servers;  ///< Postgres servers

  struct PgConfig {
    std::optional<unsigned> max_connections;
    std::optional<unsigned> shared_buffers_mb;
  };
  PgConfig pg_config;

  /// Proxy configuration
  uint16_t proxy_pg_port   = 5432;  // For client connections, that will be forwarded to the leader.
  uint16_t proxy_http_port = 8080;  // Proxy listens to a few admin commands on http port.

  /// Backups.
  struct S3 {
    string service;
    string access_id;
    string access_key;
    string region;
    string bucket;
  };
  vector<S3> s3_endpoints;

  Config() = default;
  Config(string const& filename);
};

/// Parse a toml configuration file.
inline Config config(string const& filename) {
  LOG_INFO("Reading config file: {}", filename);
  return Config{filename};
}
