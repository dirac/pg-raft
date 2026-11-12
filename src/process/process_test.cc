#define BOOST_TEST_MODULE process

#include "src/process/process.h"

#include "src/base/io_threads.h"
#include "src/base/testing.h"

// Helper that captures stdout/stderr lines instead of forwarding to stderr.
class LineCapture : public Process {
 public:
  using Process::Process;
  std::vector<std::string> out, err;

 protected:
  void on_stdout_line(std::string_view line) override {
    std::lock_guard lock{mu_};
    out.emplace_back(line);
  }
  void on_stderr_line(std::string_view line) override {
    std::lock_guard lock{mu_};
    err.emplace_back(line);
  }

 private:
  std::mutex mu_;
};

BOOST_AUTO_TEST_CASE(start_and_wait, * boost::unit_test::timeout(60)) {
  IoThreads threads{1};
  Process   p("/bin/echo", {"hello"});
  BOOST_TEST(p.start());
  BOOST_TEST(p.is_running());
  BOOST_TEST(p.wait() == 0);
  BOOST_TEST(!p.is_running());
}

BOOST_AUTO_TEST_CASE(cannot_start_while_running, * boost::unit_test::timeout(60)) {
  IoThreads threads{1};
  Process   p("/bin/sleep", {"10"});
  BOOST_TEST(p.start());
  BOOST_TEST(!p.start());
  p.kill();
  p.wait();
}

BOOST_AUTO_TEST_CASE(stop, * boost::unit_test::timeout(60)) {
  IoThreads threads{1};
  Process   p("/bin/sleep", {"10"});
  BOOST_TEST(p.start());
  p.stop();
  BOOST_TEST(!p.is_running());
}

BOOST_AUTO_TEST_CASE(try_wait_before_exit, * boost::unit_test::timeout(60)) {
  IoThreads threads{1};
  Process   p("/bin/sleep", {"10"});
  BOOST_TEST(p.start());
  BOOST_TEST(!p.try_wait().has_value());
  p.kill();
  p.wait();
}

BOOST_AUTO_TEST_CASE(try_wait_after_exit, * boost::unit_test::timeout(60)) {
  IoThreads threads{1};
  Process   p("/bin/echo", {"hello"});
  BOOST_TEST(p.start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto result = p.try_wait();
  BOOST_TEST(result.has_value());
  BOOST_TEST(*result == 0);
}

BOOST_AUTO_TEST_CASE(destructor_kills_running_process, * boost::unit_test::timeout(60)) {
  IoThreads threads{1};
  pid_t     pid;
  {
    Process p("/bin/sleep", {"10"});
    BOOST_TEST(p.start());
    pid = p.pid();
  }
  BOOST_TEST(kill(pid, 0) != 0);
}

BOOST_AUTO_TEST_CASE(capture_stdout, * boost::unit_test::timeout(60)) {
  IoThreads   threads{1};
  LineCapture p("/bin/echo", {"hello world"});
  BOOST_TEST(p.start());
  p.wait();
  BOOST_REQUIRE_EQUAL(p.out.size(), 1u);
  BOOST_TEST(p.out[0] == "hello world");
  BOOST_TEST(p.err.empty());
}

BOOST_AUTO_TEST_CASE(capture_stderr, * boost::unit_test::timeout(60)) {
  IoThreads   threads{1};
  LineCapture p("/bin/sh", {"-c", "echo err >&2"});
  BOOST_TEST(p.start());
  p.wait();
  BOOST_REQUIRE_EQUAL(p.err.size(), 1u);
  BOOST_TEST(p.err[0] == "err");
  BOOST_TEST(p.out.empty());
}

BOOST_AUTO_TEST_CASE(capture_multiple_lines, * boost::unit_test::timeout(60)) {
  IoThreads   threads{1};
  LineCapture p("/bin/sh", {"-c", "printf 'line1\\nline2\\nline3'"});
  BOOST_TEST(p.start());
  p.wait();
  BOOST_REQUIRE_EQUAL(p.out.size(), 3u);
  BOOST_TEST(p.out[0] == "line1");
  BOOST_TEST(p.out[1] == "line2");
  BOOST_TEST(p.out[2] == "line3");
}

// kill_group reaches what the child forked: a shell with a background sleep loses both.
BOOST_AUTO_TEST_CASE(kill_group_kills_descendants, *boost::unit_test::timeout(60)) {
  IoThreads threads{1};
  Process   p("/bin/sh", {"-c", "sleep 60 & sleep 60"});
  BOOST_REQUIRE(p.start());
  auto pgid = p.pid();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  BOOST_TEST(::getpgid(pgid) == pgid);  // a group of its own

  p.kill_group();
  BOOST_TEST(p.wait() == -SIGKILL);

  // The background sleep is gone too: no process is left in the group.
  bool gone = false;
  for (int i = 0; i < 50 && !gone; ++i) {
    gone = ::kill(-pgid, 0) == -1 && errno == ESRCH;
    if (!gone)
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  BOOST_TEST(gone);
}
