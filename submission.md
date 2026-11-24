# Raft Over the WAL Stream: Quorum Commit and Failover Without pg_rewind

Most PostgreSQL HA setups pair synchronous replication with an external failover manager. That
leaves a few awkward gaps. A commit waits for its standbys only after the local flush, so a
cancelled wait can make it visible before any standby has it. Failover is decided outside the
replication stream. And a demoted primary may hold WAL that no other node has.

pg-raft is an experimental system that closes these gaps by making the WAL stream itself the Raft
log. Every WAL write and flush on the primary becomes a Raft entry, captured either through a FUSE
mount over pg_wal or through four small hooks in a patched PostgreSQL 18. A flush returns only once
a majority holds its WAL. Each standby replays only committed WAL, served by a built-in
implementation of the physical replication protocol. A deposed primary rebuilds its pg_wal from the
committed log and rejoins without pg_rewind.

The talk covers the design, the failover sequence and what benchmarks across three availability
zones taught us. With the commit in the WAL flush, `WALWriteLock` serializes Raft rounds. Moving the
wait into the synchronous replication wait brings pg-raft within 1–6% of quorum synchronous
replication. We'll close with small upstream changes that would make this safer in stock
PostgreSQL.
