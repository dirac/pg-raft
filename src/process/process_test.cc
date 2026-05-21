#include "src/process/process.h"

#include <chrono>
#include <thread>

#include <signal.h>

#define BOOST_TEST_MODULE ProcessTest
#include <boost/test/included/unit_test.hpp>

BOOST_AUTO_TEST_CASE(StartAndWait) {
  Process p("/bin/echo", {"hello"});
  BOOST_TEST(p.Start());
  BOOST_TEST(p.IsRunning());
  BOOST_TEST(p.Wait() == 0);
  BOOST_TEST(!p.IsRunning());
}

BOOST_AUTO_TEST_CASE(CannotStartWhileRunning) {
  Process p("/bin/sleep", {"10"});
  BOOST_TEST(p.Start());
  BOOST_TEST(!p.Start());
  p.Kill();
  p.Wait();
}

BOOST_AUTO_TEST_CASE(Stop) {
  Process p("/bin/sleep", {"10"});
  BOOST_TEST(p.Start());
  p.Stop();
  BOOST_TEST(!p.IsRunning());
}

BOOST_AUTO_TEST_CASE(TryWaitBeforeExit) {
  Process p("/bin/sleep", {"10"});
  BOOST_TEST(p.Start());
  BOOST_TEST(!p.TryWait().has_value());
  p.Kill();
  p.Wait();
}

BOOST_AUTO_TEST_CASE(TryWaitAfterExit) {
  Process p("/bin/echo", {"hello"});
  BOOST_TEST(p.Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto result = p.TryWait();
  BOOST_TEST(result.has_value());
  BOOST_TEST(*result == 0);
}

BOOST_AUTO_TEST_CASE(DestructorKillsRunningProcess) {
  pid_t pid;
  {
    Process p("/bin/sleep", {"10"});
    BOOST_TEST(p.Start());
    pid = p.pid();
  }
  BOOST_TEST(kill(pid, 0) != 0);
}

BOOST_AUTO_TEST_CASE(MoveTransfersOwnership) {
  Process p1("/bin/sleep", {"10"});
  BOOST_TEST(p1.Start());
  pid_t pid = p1.pid();

  Process p2 = std::move(p1);
  BOOST_TEST(!p1.IsRunning());
  BOOST_TEST(p2.IsRunning());
  BOOST_TEST(p2.pid() == pid);

  p2.Kill();
  p2.Wait();
}
