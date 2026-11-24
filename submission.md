# Raft Over the WAL Stream: Quorum Commit and Failover Without pg_rewind

Most PostgreSQL HA deployments combine synchronous replication with an external failover manager.
When the primary server commits a transaction, it waits for its standbys only after flushing the
commit record, so a user cancellation or other interruption can make the commit visible on the
primary before any standby has it. This violates the synchronous commit guarantee and can lead to
inconsistency after failover. If the primary server crashes in this scenario, it may hold WAL that
no other node has: the external failover manager will promote a standby that is missing a
transaction other sessions have already seen, and the old primary needs pg_rewind before it can
rejoin.

pg-raft is an experimental system that solves these problems by wrapping the WAL stream in a Raft
log. Every WAL write and flush on the primary becomes a Raft entry, captured either through a FUSE
mount over pg_wal or through four small hooks in a patched PostgreSQL 18. A commit returns only
after a majority of nodes have replicated its log entries. Each standby replays only committed WAL,
served by an implementation of the physical replication protocol. A dead primary rebuilds its
pg_wal from the committed log when it restarts and rejoins the quorum without pg_rewind.

The talk covers the design, the failover sequence and benchmarks across three availability
zones. At pgbench scale 1000, pg-raft reaches 91–97% of the throughput of synchronous
replication. A commit can wait for Raft inside the WAL flush, where `WALWriteLock` serializes
Raft rounds, or in the synchronous replication wait, where rounds overlap: we'll show how each 
of these performs. We'll conclude with a description of the patches.
