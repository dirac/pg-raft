#include "src/base/crc32c.h"

#include <array>
#include <cstring>

#if defined(__aarch64__)
#include <arm_acle.h>
#if defined(__linux__)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif
#elif defined(__x86_64__)
#include <nmmintrin.h>
#endif

namespace {

constexpr uint32_t kPoly = 0x82F63B78U;  // reflected Castagnoli polynomial

constexpr std::array<uint32_t, 256> make_table() {
  std::array<uint32_t, 256> t{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k)
      c = (c & 1U) ? (c >> 1U) ^ kPoly : c >> 1U;
    t[i] = c;
  }
  return t;
}

constexpr auto kTable = make_table();

uint32_t crc32c_sw(uint32_t crc, unsigned char const* p, size_t n) {
  while (n--)
    crc = kTable[(crc ^ *p++) & 0xFFU] ^ (crc >> 8U);
  return crc;
}

#if defined(__aarch64__)
__attribute__((target("+crc"))) uint32_t crc32c_hw(uint32_t crc, unsigned char const* p, size_t n) {
  while (n >= 8) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    crc = __crc32cd(crc, v);
    p += 8;
    n -= 8;
  }
  while (n--)
    crc = __crc32cb(crc, *p++);
  return crc;
}

bool have_hw() {
#if defined(__linux__)
  return (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
#else
  return true;  // every ARMv8 Apple CPU has it
#endif
}
#elif defined(__x86_64__)
__attribute__((target("sse4.2"))) uint32_t crc32c_hw(uint32_t             crc,
                                                     unsigned char const* p,
                                                     size_t               n) {
  while (n >= 8) {
    uint64_t v;
    std::memcpy(&v, p, 8);
    crc = static_cast<uint32_t>(_mm_crc32_u64(crc, v));
    p += 8;
    n -= 8;
  }
  while (n--)
    crc = _mm_crc32_u8(crc, *p++);
  return crc;
}

bool have_hw() {
  // __builtin_cpu_supports reads the CPU model the runtime fills in: in a static initializer that
  // may not have run yet (it answered false on an SSE4.2 CPU), so initialize it first.
  __builtin_cpu_init();
  return __builtin_cpu_supports("sse4.2");
}
#else
uint32_t crc32c_hw(uint32_t crc, unsigned char const* p, size_t n) {
  return crc32c_sw(crc, p, n);
}
bool have_hw() {
  return false;
}
#endif

}  // namespace

bool crc32c_hardware() {
  static bool const hw = have_hw();  // on first use, not in a static initializer
  return hw;
}

uint32_t crc32c(uint32_t crc, void const* data, size_t size) {
  bool const  kHw = crc32c_hardware();
  auto const* p   = static_cast<unsigned char const*>(data);
  crc           = ~crc;
  crc           = kHw ? crc32c_hw(crc, p, size) : crc32c_sw(crc, p, size);
  return ~crc;
}

uint32_t crc32c_portable(uint32_t crc, void const* data, size_t size) {
  return ~crc32c_sw(~crc, static_cast<unsigned char const*>(data), size);
}
