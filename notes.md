Raft Over the WAL Stream: Notes

The leader and the Raft log

A pg-raft deployment is one Raft group with one node per server. Each node runs a Raft server and a local PostgreSQL. The Raft leader's PostgreSQL is the primary, and every other PostgreSQL is a standby.

The WAL is the log. Each of the primary's WAL writes, fsyncs and timeline switches becomes a Raft entry, in the order PostgreSQL makes them. The leader stores each entry with its WAL bytes in its log, and the followers receive it byte for byte.

An entry commits when a majority of the nodes have it. PostgreSQL's WAL fsync returns only once its entry has committed, so no commit becomes visible on the primary before a quorum has it.

Every raft node writes the WAL of each committed entry into its own copy of the WAL and streams it to its own standby over PostgreSQL's physical replication protocol. A standby never replays WAL that wasn't committed.

FUSE: intercepting the WAL writes and fsyncs

A FUSE filesystem is mounted over the primary's pg_wal directory. PostgreSQL is unmodified and reads and writes its WAL files as usual.

A write stores the bytes in the file and hands them to the node for the next Raft entry. An fsync becomes a Raft entry, and the fsync returns only once that entry has committed, or fails if the node has lost leadership. A new timeline's history file, written on promotion, becomes a timeline entry. Every other file operation passes straight through.

The alternative to FUSE is a few small hooks in a patched PostgreSQL 18, at the same points: WAL write, WAL flush and timeline switch. They produce the same Raft entries, and in the benchmark below FUSE and the hooks perform the same.

Controlling the PostgreSQL process

The node starts, promotes and stops its PostgreSQL itself, following its Raft role.

PostgreSQL always starts as a standby, streaming from its raft node. A node without a usable data directory first restores the latest backup.

When the node becomes leader, its standby replays to the end of the committed WAL, the node promotes it, and the new timeline is committed through Raft. Only then does the node accept clients as the primary. If any step fails, the node steps down.

When the node loses leadership, it stops its PostgreSQL at once, and every fsync still waiting fails. No client of the old primary gets an answer after this point.

The old primary rejoins without pg_rewind. Its data directory never runs ahead of the committed WAL, because PostgreSQL writes a data page only after the WAL covering it has been flushed, and here a flush means committed. Only its pg_wal can hold uncommitted WAL, so the node rebuilds pg_wal from the committed copy and restarts PostgreSQL as a standby.

Results: pgbench at scale 1000 across three availability zones

Three database servers ran in three availability zones of AWS us-west-2, with pgbench on a fourth machine, PostgreSQL 18.7, pgbench's TPC-B-like workload at scale 1000 and 300 clients. The numbers are means of 5 rounds.

Standard synchronous replication, with a primary and two standbys and a commit waiting for one of them, reached 50,519 transactions per second. pg-raft with three nodes reached 49,136 with FUSE and 49,190 with the hooks, 97% of synchronous replication, with about 0.3 ms more per commit. A single PostgreSQL without HA reached 56,271 in one run.

A separate two-hour fault-injection run lost no acknowledged write, and clients reached a new
primary a median of 1.5 seconds after a leader failure.
