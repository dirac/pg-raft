#pragma once

#include "src/base/base.h"

struct ChecksumContext {
  uint8_t  data[64];
  uint32_t datalen;
  uint64_t bitlen;
  uint32_t state[8];
};

struct Checksum : public std::array<uint8_t, 32> {
  operator std::string() {
    std::string result;
    result.reserve(65);

    return std::accumulate(
        std::begin(*this), std::end(*this), std::move(result), [](std::string result, uint8_t e) {
          result += std::format("{:02x}", static_cast<int>(e));
          return result;
        });
  }
};

void     init(ChecksumContext* ctx);
void     update(ChecksumContext* ctx, void const* data, size_t bytes);
Checksum finalize(ChecksumContext* ctx);
