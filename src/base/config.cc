#include "src/base/config.h"

#include <toml.hpp>

#include "src/base/logger.h"

namespace {

template <typename T>
void parse_dir(T& data, Config& config) {
  if (data.contains("dir"))
    config.wdir = toml::find<string>(data, "dir");
}

template <typename T>
void parse_s3(T& data, vector<Config::S3>& s3_endpoints) {
  Config::S3 e;

  e.service    = toml::find<string>(data, "service");
  e.access_id  = toml::find<string>(data, "access_id");
  e.access_key = toml::find<string>(data, "access_key");
  e.bucket     = toml::find<string>(data, "bucket");
  e.region     = toml::find<string>(data, "region");

  s3_endpoints.emplace_back(std::move(e));
}

template <typename T>
void parse_s3_vec(T& data, Config& config) {
  if (!data.contains("s3") || !data.at("s3").is_array_of_tables())
    return;

  for (size_t i = 0; i < data.at("s3").size(); ++i)
    parse_s3(data.at("s3").at(i), config.s3_endpoints);
}

template <typename T>
void parse_server(T& data, vector<Config::Server>& servers) {
  Config::Server e;

  e.hostname = toml::find<string>(data, "hostname");
  e.pg_port  = toml::find<uint16_t>(data, "pg_port");
  e.ha_port  = toml::find<uint16_t>(data, "ha_port");

  servers.emplace_back(std::move(e));
}

template <typename T>
void parse_server_vec(T& data, Config& config) {
  if (!data.contains("server") || !data.at("server").is_array_of_tables())
    return;

  for (size_t i = 0; i < data.at("server").size(); ++i)
    parse_server(data.at("server").at(i), config.servers);
}

template <typename T>
void parse_pg_config(T& data, Config& config) {
  if (!data.contains("pg_config") || !data.at("pg_config").is_table())
    return;

#define PROP(type, nm)                    \
  if (data.at("pg_config").contains(#nm)) \
  config.pg_config.nm = toml::find<type>(data.at("pg_config"), #nm)

  PROP(unsigned, max_connections);
}

}  // namespace

Config::Config(string const& filename) {
  auto data = toml::parse(filename);

  parse_dir(data, *this);
  parse_s3_vec(data, *this);
  parse_server_vec(data, *this);
  parse_pg_config(data, *this);
}
