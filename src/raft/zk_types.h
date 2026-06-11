#pragma once

#include "src/base/base.h"

struct OpenOp {
  /// The 24-character filename is divided into three distinct 8-digit (4-byte) structural segments.
  LogSeqNumber lsn;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & lsn;
  }
};

struct WriteOp {
  LogSegNumber lsn;
  uint32_t     offset;
  uint32_t     length;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & lsn;
    ar & offset;
    ar & length;
  }
};

struct CloseOp {
  LogSeqNumber lsn;

  template <typename Archive>
  void serialize(Archive& ar, unsigned int /* version */) {
    ar & lsn;
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
