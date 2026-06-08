#ifndef LIB_SERVERS_ZOOKEEPER_OPTIONS_HPP_
#define LIB_SERVERS_ZOOKEEPER_OPTIONS_HPP_

#include "lib/base/base.hpp"

namespace banyandb::servers::zookeeper {

using namespace banyandb::base;

struct Options {
  unsigned election_timeout   = 200;
  unsigned max_entries        = 0x1 << 13;  // Max enteries to send in an append entries request.
  unsigned tick_seconds       = 2;          // Every two seconds we add a tick entry.
  unsigned snapshot_lag       = 0x1 << 13;  // Create a new snapshot if it exceeds the lag, 8K
  unsigned keep_old_logs      = 0x1 << 14;  // Keep older logs upto when compacting.
  unsigned compaction_lag     = 0x1 << 16;  // The follower will compact the log, 65k
  unsigned timeout_secs       = 10;
  unsigned reconnect_interval = 2000;  // Attempt reconnect after 2 seconds.
  bool     log_req_response   = false;
};

}  // namespace banyandb::servers::zookeeper

#endif
