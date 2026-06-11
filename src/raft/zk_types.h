#pragma once

#include "src/base/base.h"

struct OpenOp {
  /// The 24-character filename is divided into three distinct 8-digit (4-byte) structural segments.
  uint32_t tli;
  uint32_t log_id;
  uint32_t seg_id;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & tli;
    ar & log_id;
    ar & seg_id;
  }
};

struct WriteOp {
  uint32_t tli;
  uint32_t log_id;
  uint32_t seg_id;
  uint32_t offset;
  uint32_t length;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & tli;
    ar & log_id;
    ar & seg_id;
    ar & offset;
    ar & length;
  }
};

struct CloseOp {
  uint32_t tli;
  uint32_t log_id;  //< High 32 bits of lsn
  uint32_t seg_id;  //< Lower 32 bits of lsn.

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & tli;
    ar & log_id;
    ar & seg_id;
  }
};

struct FlushOp {
  template <typename Archive>
  void serialize(Archive& /*ar*/, unsigned int /* version */) {}
};

struct WalCommand {
  uint64_t                                        flushed_lsn;
  std::variant<OpenOp, WriteOp, FlushOp, CloseOp> command;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & flushed_lsn;
    ar & command;
  }
};
