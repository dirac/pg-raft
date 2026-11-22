#pragma once

#include "src/base/base.h"
#include "src/base/config.h"

char const* top_dir();

/// The PostgreSQL binaries the integration tests run (Config's default major version); the tests
/// skip themselves when they are absent.
inline std::string const kPgTestBin =
    std::format("/opt/pg_raft/postgresql/{}/bin", Config{}.pg_major_version);

/// A node test's Config from the environment: PG_RAFT_WAL_CAPTURE (fuse, hook), PG_RAFT_INSTALL_DIR
/// (pg-raft's PostgreSQL for hook: <repo>/build/postgresql, external/build-pg-raft-postgresql.sh),
/// PG_RAFT_COMMIT_WAIT (fsync, sync_rep).
inline void config_from_env(Config& cfg) {
  if (auto const* c = std::getenv("PG_RAFT_WAL_CAPTURE"); c)
    cfg.wal_capture = c;
  if (auto const* d = std::getenv("PG_RAFT_INSTALL_DIR"); d)
    cfg.install_dir = d;
  if (auto const* w = std::getenv("PG_RAFT_COMMIT_WAIT"); w)
    cfg.commit_wait = w;
}

/// Load the config from config.toml. Load S3 info is loaded from
/// $HOME/.pg_raft/testing.toml"
Config test_config();

// Creating TmpDir
struct TmpDir {
  TmpDir(bool remove = true);
  ~TmpDir();

  TmpDir(TmpDir const&)            = delete;
  TmpDir& operator=(TmpDir const&) = delete;

  std::filesystem::path const& operator()() const { return path_; }

  void echo() const {
    for (auto const& dir_entry : std::filesystem::recursive_directory_iterator(path_))
      std::cout << dir_entry << '\n';
  }

 private:
  bool                  rm_;
  std::filesystem::path path_;
};

#ifndef CLANG_TIDY
#ifndef BOOST_TEST_MODULE
#ifndef DONT_DEFINE_BOOST_TEST_MODULE
#error #define BOOST_TEST_MODULE module_name is required in your test file.
#endif
#endif
#endif

#include <boost/mpl/list.hpp>
#include <boost/test/data/monomorphic.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/test_case_template.hpp>
#include <boost/test/unit_test.hpp>

