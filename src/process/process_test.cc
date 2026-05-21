#include "src/process/process.h"

#include <chrono>
#include <thread>

#include <signal.h>

#define BOOST_TEST_MODULE ProcessTest
#include <boost/test/included/unit_test.hpp>

BOOST_AUTO_TEST_CASE(StartAndWait) {
  Process p("/bin/echo", {"hello"});
  BOOST_TEST(p.start());
  BOOST_TEST(p.is_running());
  BOOST_TEST(p.wait() == 0);
  BOOST_TEST(!p.is_running());
}

BOOST_AUTO_TEST_CASE(CannotStartWhileRunning) {
  Process p("/bin/sleep", {"10"});
  BOOST_TEST(p.start());
  BOOST_TEST(!p.start());
  p.kill();
  p.wait();
}

BOOST_AUTO_TEST_CASE(Stop) {
  Process p("/bin/sleep", {"10"});
  BOOST_TEST(p.start());
  p.stop();
  BOOST_TEST(!p.is_running());
}

BOOST_AUTO_TEST_CASE(TryWaitBeforeExit) {
  Process p("/bin/sleep", {"10"});
  BOOST_TEST(p.start());
  BOOST_TEST(!p.try_wait().has_value());
  p.kill();
  p.wait();
}

BOOST_AUTO_TEST_CASE(TryWaitAfterExit) {
  Process p("/bin/echo", {"hello"});
  BOOST_TEST(p.start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto result = p.try_wait();
  BOOST_TEST(result.has_value());
  BOOST_TEST(*result == 0);
}

BOOST_AUTO_TEST_CASE(DestructorKillsRunningProcess) {
  pid_t pid;
  {
    Process p("/bin/sleep", {"10"});
    BOOST_TEST(p.start());
    pid = p.pid();
  }
  BOOST_TEST(kill(pid, 0) != 0);
}

BOOST_AUTO_TEST_CASE(MoveTransfersOwnership) {
  Process p1("/bin/sleep", {"10"});
  BOOST_TEST(p1.start());
  pid_t pid = p1.pid();

  Process p2 = std::move(p1);
  BOOST_TEST(!p1.is_running());
  BOOST_TEST(p2.is_running());
  BOOST_TEST(p2.pid() == pid);

  p2.kill();
  p2.wait();
}
