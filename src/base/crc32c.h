#pragma once

#include <cstddef>
#include <cstdint>

// CRC-32C (Castagnoli, the polynomial of iSCSI, ext4 and SSE4.2's crc32), on the CPU's instructions
// when it has them (ARMv8's CRC extension, x86's SSE4.2), a table otherwise.
//
// Incremental: crc32c(crc32c(0, a, n), b, m) == crc32c(0, a + b, n + m) (a + b: the concatenation).

uint32_t crc32c(uint32_t crc, void const* data, size_t size);

/// Whether crc32c() runs on the CPU's instructions.
bool crc32c_hardware();

/// The same, always on the table (what a CPU without the instructions runs; for tests).
uint32_t crc32c_portable(uint32_t crc, void const* data, size_t size);
