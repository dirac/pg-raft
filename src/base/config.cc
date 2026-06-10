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

struct type_name_of {
  std::string operator()(toml::value::boolean_type const&) const { return "boolean"; }
  std::string operator()(toml::value::integer_type const&) const { return "integer"; }
  std::string operator()(toml::value::floating_type const&) const { return "floating"; }
  std::string operator()(toml::value::string_type const&) const { return "string"; }
  std::string operator()(toml::value::local_time_type const&) const { return "local_time"; }
  std::string operator()(toml::value::local_date_type const&) const { return "local_date"; }
  std::string operator()(toml::value::local_datetime_type const&) const { return "local_datetime"; }
  std::string operator()(toml::value::offset_datetime_type const&) const {
    return "offset_datetime";
  }
  std::string operator()(toml::value::array_type const&) const { return "array"; }
  std::string operator()(toml::value::table_type const&) const { return "table"; }
};

std::string to_string(toml::value const& v) {
  switch (v.type()) {
    case toml::value_t::integer:
      return format("{}", v.as_integer());
    case toml::value_t::floating:
      return format("{}", v.as_float());
    case toml::value_t::string:
      return v.as_string();
  }
  throw "unsupported type";
}

template <typename T>
void parse_pg_config(T& data, Config& config) {
  if (!data.contains("pg_config") || !data.at("pg_config").is_table())
    return;

  auto const& pg_config = data.at("pg_config");
  for (auto const& [key, val] : pg_config) {
    std::cout << key << "=" << toml::visit(type_name_of{}, val) << " " << to_string(val)
              << std::endl;
  }
}

}  // namespace

Config::Config(string const& filename) {
  auto data = toml::parse(filename);

  parse_dir(data, *this);
  parse_server_vec(data, *this);
  parse_pg_config(data, *this);
}
