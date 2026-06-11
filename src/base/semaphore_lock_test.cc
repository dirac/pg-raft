#define BOOST_TEST_MODULE semaphore_lock

#include "src/base/semaphore_lock.h"
#include "src/base/testing.h"

// ── constructor (try_acquire) ─────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(constructor_acquires_free_semaphore, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{1};
  SemaphoreLock         lk{sem};
  BOOST_TEST(lk.owns());
  BOOST_TEST(bool(lk));
}

BOOST_AUTO_TEST_CASE(constructor_does_not_block_when_taken, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{0};
  SemaphoreLock         lk{sem};
  BOOST_TEST(!lk.owns());
  BOOST_TEST(!bool(lk));
}

// ── destructor ────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(destructor_releases, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{1};
  { SemaphoreLock lk{sem}; }
  // Semaphore should be available again after the lock is destroyed.
  BOOST_TEST(sem.try_acquire());
}

BOOST_AUTO_TEST_CASE(destructor_does_not_release_if_not_owned, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{0};
  { SemaphoreLock lk{sem}; }
  // Semaphore was never acquired, still at 0 — try_acquire should fail.
  BOOST_TEST(!sem.try_acquire());
}

// ── acquire ───────────────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(acquire_noop_when_already_owned, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{1};
  SemaphoreLock         lk{sem};
  BOOST_REQUIRE(lk.owns());
  lk.acquire();  // must not block or double-release
  BOOST_TEST(lk.owns());
}

BOOST_AUTO_TEST_CASE(acquire_blocks_until_released, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{0};
  SemaphoreLock         lk{sem};
  BOOST_REQUIRE(!lk.owns());

  // Release the semaphore from another thread after a short delay.
  std::thread t([&sem] {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    sem.release();
  });

  lk.acquire();
  BOOST_TEST(lk.owns());
  t.join();
}

// ── move constructor ──────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(move_constructor_transfers_ownership, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{1};
  SemaphoreLock         a{sem};
  BOOST_REQUIRE(a.owns());

  SemaphoreLock b{std::move(a)};
  BOOST_TEST(!a.owns());
  BOOST_TEST(b.owns());
}

BOOST_AUTO_TEST_CASE(move_constructor_from_unowned, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{0};
  SemaphoreLock         a{sem};
  BOOST_REQUIRE(!a.owns());

  SemaphoreLock b{std::move(a)};
  BOOST_TEST(!a.owns());
  BOOST_TEST(!b.owns());
}

// ── move assignment ───────────────────────────────────────────────────────────

BOOST_AUTO_TEST_CASE(move_assignment_transfers_ownership, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{1};
  SemaphoreLock         a{sem};
  BOOST_REQUIRE(a.owns());

  std::binary_semaphore sem2{1};
  SemaphoreLock         b{sem2};
  BOOST_REQUIRE(b.owns());

  // Assigning to b should release sem2 and take ownership of sem.
  b = std::move(a);
  BOOST_TEST(!a.owns());
  BOOST_TEST(b.owns());
  // sem2 was released by the assignment — should be acquirable again.
  BOOST_TEST(sem2.try_acquire());
}

BOOST_AUTO_TEST_CASE(move_assignment_from_unowned, * boost::unit_test::timeout(60)) {
  std::binary_semaphore sem{0};
  SemaphoreLock         a{sem};
  BOOST_REQUIRE(!a.owns());

  std::binary_semaphore sem2{1};
  SemaphoreLock         b{sem2};
  BOOST_REQUIRE(b.owns());

  b = std::move(a);
  BOOST_TEST(!a.owns());
  BOOST_TEST(!b.owns());
  // sem2 released by the assignment.
  BOOST_TEST(sem2.try_acquire());
}
