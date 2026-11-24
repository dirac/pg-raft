# Raft Over the WAL Stream: Quorum Commit and Failover Without pg_rewind

Most PostgreSQL HA deployments combine synchronous replication with an external failover manager. 
When the primary server commits a transaction, it waits for its standbys only after flushing the 
commit record, so a user cancellation or other kind of interruption of the transaction can make 
the commit visible on the primary before any standby has it. This violates the synchronous commit 
guarantee and can lead to consistency issues later on. If the primary server crashes in this scenario, 
it may hold WAL that no other node has. The external failover manager will promote a standby that 
is missing a committed transaction.

pg-raft is an experimental system that solves these problems by wrapping the WAL stream in a Raft
log. Every WAL write and flush on the primary becomes a Raft entry, captured either through a FUSE
mount over pg_wal or through four small hooks in a patched PostgreSQL 18. A flush (fsync) returns only 
after a majority of nodes have replicated the log entries. Each standby replays only committed WAL, 
served by an implementation of the physical replication protocol. A dead primary rebuilds its 
pg_wal from the committed log when it restarts and rejoins the quorum without pg_rewind.

The talk covers the design, the failover sequence and benchmarks across three availability
zones. With the commit in the WAL flush, `WALWriteLock` serializes Raft rounds. Moving the
wait into the synchronous replication wait brings pg-raft within 1–6% of quorum synchronous
replication. We'll close with small upstream changes that would make this safer in stock
PostgreSQL.
