#define DONT_DEFINE_BOOST_TEST_MODULE
#define BOOST_TEST_MAIN 1
#define BOOST_TEST_ALTERNATIVE_INIT_API

#include "src/base/testing.h"

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <toml.hpp>

#define stringize(s) xstringize(s)
#define xstringize(s) #s

char const* top_dir() {
  return stringize(TOP_DIR);
}

Config test_config() {
  auto config = Config{std::format("{0}/src/base/config.toml", top_dir())};

  {
    auto s3 = std::filesystem::path{std::string{getenv("HOME")} + "/.pg_raft/testing.toml"};

    std::error_code ec;
    if (!std::filesystem::exists(s3, ec) || ec)
      fatal("The file {}/.pg_raft/testing.toml doesn't exist", getenv("HOME"));

    if (!std::filesystem::is_regular_file(s3, ec) || ec)
      fatal("The file {}/.pg_raft/testing.toml isn't a regular file.", getenv("HOME"));
    config.s3_endpoints = Config{s3.c_str()}.s3_endpoints;
  }
  return config;
}

string test_name() {
  return string{boost::unit_test::framework::current_test_case().full_name()};
}

namespace {
struct TestBasePath {
  TestBasePath() = default;

  ~TestBasePath() {
    if (base_path_.empty())
      return;

    if (remove_) {
      std::error_code ec;
      std::filesystem::remove_all(base_path_, ec);
      if (ec)
        error("Unable to delete test directory: {}", base_path_.c_str());
    } else
      info("Base test directory: {}", base_path_.c_str());
  }

  std::filesystem::path path() {
    if (base_path_.empty())
      base_path_ = construct_base();
    return base_path_;
  }

  void set_remove(bool r) { remove_ = remove_ && r; }

 private:
  static std::filesystem::path construct_base();

  bool                  remove_ = true;
  std::filesystem::path base_path_;
};

std::filesystem::path TestBasePath::construct_base() {
  using namespace std::chrono_literals;
  while (true) {
    std::time_t t  = std::time(nullptr);
    std::tm     tm = *std::localtime(&t);

    stringstream ss;
    ss << std::put_time(&tm, "%y-%m-%d-%H%M%S");

    auto p = std::filesystem::temp_directory_path() / "pg_raft" / ss.str();

    if (!std::filesystem::exists(p)) {
      std::filesystem::create_directories(p);
      return p;
    }
    std::this_thread::sleep_for(1000ms);
  }
}
TestBasePath base_path;
}  // namespace

TmpDir::TmpDir(bool remove) : rm_(remove) {
  base_path.set_remove(rm_);

  string name{boost::unit_test::framework::current_test_case().full_name()};

  stringstream ss;
  ss << boost::unit_test::framework::current_test_case().p_file_name;
  string file = ss.str();

  path_ = base_path.path() / file / name;

  std::filesystem::create_directories(path_);
  if (!rm_)
    info("Created: {}", path_.c_str());
}

TmpDir::~TmpDir() {
  if (rm_)
    std::filesystem::remove_all(path_);
}

#include <boost/test/framework.hpp>

struct S : boost::unit_test::test_observer {
  using test_unit = boost::unit_test::test_unit;

  void start() {}
  void finish() {}

  void test_unit_start(test_unit const&) final { start(); }
  void test_unit_finish(test_unit const&, unsigned long) final { finish(); }
  void test_unit_skipped(test_unit const&) final { finish(); }
  void test_unit_timed_out(test_unit const&) final { finish(); }
  void test_unit_aborted(test_unit const&) final { finish(); }

  int priority() final { return std::numeric_limits<int>::max(); }
};

int main(int argc, char* argv[], char* envp[]) {
  (void)envp;
  if (char const* skip = getenv("SKIP_TESTS"); skip != nullptr && strcmp(skip, "true") == 0) {
    info("Skipping test because the environment variable SKIP_TESTS is set : {}", argv[0]);
    return -1;
  }

  S s{};
  boost::unit_test::framework::register_observer(s);
  auto r = boost::unit_test::unit_test_main(init_unit_test, argc, argv);
  return r;
}
