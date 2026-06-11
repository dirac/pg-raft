#pragma once

#include "src/base/base.h"

// RAII guard for std::binary_semaphore, analogous to std::unique_lock.
//
// The constructor does a non-blocking try_acquire. Call acquire() to block
// until the semaphore is obtained if the constructor did not succeed.
// The destructor releases the semaphore iff it was acquired.
class SemaphoreLock {
 public:
  explicit SemaphoreLock(std::binary_semaphore& sem)
      : sem_(&sem), acquired_(sem_->try_acquire()) {}

  ~SemaphoreLock() {
    if (acquired_)
      sem_->release();
  }

  SemaphoreLock(SemaphoreLock&& other) noexcept
      : sem_(other.sem_), acquired_(other.acquired_) {
    other.sem_      = nullptr;
    other.acquired_ = false;
  }

  SemaphoreLock& operator=(SemaphoreLock&& other) noexcept {
    if (this != &other) {
      if (acquired_)
        sem_->release();
      sem_           = other.sem_;
      acquired_      = other.acquired_;
      other.sem_     = nullptr;
      other.acquired_ = false;
    }
    return *this;
  }

  SemaphoreLock(SemaphoreLock const&)            = delete;
  SemaphoreLock& operator=(SemaphoreLock const&) = delete;

  // Blocks until the semaphore is acquired. No-op if already held.
  void acquire() {
    if (!acquired_) {
      sem_->acquire();
      acquired_ = true;
    }
  }

  // Explicitly release the semaphore before destruction. No-op if not owned.
  void release() {
    if (acquired_) {
      sem_->release();
      acquired_ = false;
    }
  }

  bool owns() const { return acquired_; }
  explicit operator bool() const { return acquired_; }

 private:
  std::binary_semaphore* sem_;
  bool                   acquired_;
};
