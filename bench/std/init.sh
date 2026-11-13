#!/bin/bash
# Single machine: the primary and both standbys here, then wait for the quorum.
# (On several machines: init_primary.sh on the primary's, init_standby.sh N on each standby's,
# then wait.sh on the client machine.)
set -euo pipefail
source "$(dirname "$0")/lib.sh"

"$STD_DIR/init_primary.sh"
for ((id = 1; id < NODE_COUNT; id++)); do "$STD_DIR/init_standby.sh" "$id"; done
"$STD_DIR/wait.sh"
