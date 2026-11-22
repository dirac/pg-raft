/*
 * pg_raft_wal: the WAL a primary writes, handed to the pg-raft node before a flush returns.
 *
 * Loaded by the node (shared_preload_libraries in pg_raft.conf, with pg_raft.socket). Implements
 * the hooks of PostgreSQL's pg-raft patch (wal_write_hook, wal_flush_hook, wal_timeline_hook in
 * access/xlog.h): each is one request to the node over a Unix socket, and one byte back.
 *
 *  - write:    the WAL at [offset, offset + size) of a segment is in the file; the node takes it
 *              (from the file) and answers at once.
 *  - flush:    the WAL of a segment up to an offset must be durable: the node answers when raft has
 *              committed it. Then the local fsync is skipped (pg_wal is a cache: raft keeps the
 *              WAL). A failure is a PANIC, as a failed fsync is: nothing the node didn't commit may
 *              be acknowledged.
 *  - timeline: a new timeline's history file is in pg_wal (on promotion); its content goes along.
 *
 * The write and flush hooks run with WALWriteLock held, so the requests of every process reach the
 * node one at a time, in WAL order. They run inside a critical section: no palloc, no ERROR (only
 * plain system calls here, and PANIC on failure). Each process connects on its first request.
 * A standby never writes WAL through XLogWrite: the hooks only run on a primary.
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* The request: 24 bytes, native byte order (the node runs on the same machine). */
typedef struct WalRequest
{
	uint8		type;			/* 'W', 'F', 'T' */
	uint8		pad[3];
	uint32		tli;
	uint64		segno;
	uint32		offset;			/* W: start; F: end */
	uint32		size;			/* W: bytes; T: content bytes that follow */
} WalRequest;

StaticAssertDecl(sizeof(WalRequest) == 24, "WalRequest is 24 bytes");

static char *socket_path = NULL;
static int	sock = -1;
static pid_t sock_pid = 0;

static bool
write_all(int fd, const void *buf, size_t n)
{
	const char *p = buf;

	while (n > 0)
	{
		ssize_t		w = send(fd, p, n, 0);

		if (w < 0 && errno == EINTR)
			continue;
		if (w <= 0)
			return false;
		p += w;
		n -= (size_t) w;
	}
	return true;
}

static bool
read_byte(int fd, uint8 *out)
{
	for (;;)
	{
		ssize_t		r = recv(fd, out, 1, 0);

		if (r < 0 && errno == EINTR)
			continue;
		return r == 1;
	}
}

static void
disconnect(void)
{
	if (sock >= 0)
		close(sock);
	sock = -1;
}

/* The connection to the node, made on first use in each process. */
static bool
connected(void)
{
	struct sockaddr_un addr;
	int			fd;
	int			one = 1;

	if (sock >= 0 && sock_pid == getpid())
		return true;
	if (sock >= 0)				/* inherited across fork: not ours */
		sock = -1;
	if (socket_path == NULL || socket_path[0] == '\0' ||
		strlen(socket_path) >= sizeof(addr.sun_path))
		return false;

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0)
		return false;
#ifdef SO_NOSIGPIPE
	(void) setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#else
	(void) one;
#endif
	(void) fcntl(fd, F_SETFD, FD_CLOEXEC);
	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, socket_path);
	while (connect(fd, (struct sockaddr *) &addr, sizeof(addr)) != 0)
	{
		if (errno == EINTR)
			continue;
		close(fd);
		return false;
	}
	sock = fd;
	sock_pid = getpid();
	return true;
}

/*
 * One request (and |extra| bytes after it), and the node's answer: true if it answered 0. A lost
 * connection is retried once (the node may have restarted its server).
 */
static bool
request(const WalRequest *req, const char *extra, size_t extra_len)
{
	for (int attempt = 0; attempt < 2; attempt++)
	{
		uint8		answer;

		if (!connected())
			return false;
		if (write_all(sock, req, sizeof(*req)) &&
			(extra_len == 0 || write_all(sock, extra, extra_len)) &&
			read_byte(sock, &answer))
			return answer == 0;
		disconnect();
		/* A flush must not be sent twice blindly: only retry writes and timelines. */
		if (req->type == 'F')
			return false;
	}
	return false;
}

static void
pg_raft_wal_write(TimeLineID tli, XLogSegNo segno, uint32 offset, uint32 size, const char *data)
{
	WalRequest	req = {.type = 'W', .tli = tli, .segno = segno, .offset = offset, .size = size};

	(void) data;				/* the node reads the WAL from the segment file */
	if (!request(&req, NULL, 0))
		elog(PANIC, "pg_raft_wal: the node did not take the WAL at %u of segment %llu of timeline %u",
			 offset, (unsigned long long) segno, tli);
}

static bool
pg_raft_wal_flush(TimeLineID tli, XLogSegNo segno, uint32 end_offset)
{
	WalRequest	req = {.type = 'F', .tli = tli, .segno = segno, .offset = end_offset};

	if (!request(&req, NULL, 0))
		elog(PANIC, "pg_raft_wal: the WAL up to %u of segment %llu of timeline %u was not committed",
			 end_offset, (unsigned long long) segno, tli);
	return true;				/* durable by replication: no local fsync */
}

static void
pg_raft_wal_timeline(TimeLineID tli)
{
	char		path[MAXPGPATH];
	char	   *content;
	ssize_t		n = 0;
	int			fd;
	off_t		size;
	WalRequest	req = {.type = 'T', .tli = tli};

	TLHistoryFilePath(path, tli);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		elog(PANIC, "pg_raft_wal: could not open \"%s\": %m", path);
	size = lseek(fd, 0, SEEK_END);
	content = malloc(size > 0 ? (size_t) size : 1);
	if (content == NULL || size < 0)
		elog(PANIC, "pg_raft_wal: could not read \"%s\"", path);
	while (n < size)
	{
		ssize_t		r = pread(fd, content + n, (size_t) (size - n), n);

		if (r < 0 && errno == EINTR)
			continue;
		if (r <= 0)
			elog(PANIC, "pg_raft_wal: could not read \"%s\": %m", path);
		n += r;
	}
	close(fd);

	req.size = (uint32) size;
	if (!request(&req, content, (size_t) size))
		elog(PANIC, "pg_raft_wal: the node did not take the history of timeline %u", tli);
	free(content);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("pg_raft_wal must be loaded by shared_preload_libraries")));

	DefineCustomStringVariable("pg_raft.socket",
							   "The pg-raft node's WAL socket.",
							   NULL,
							   &socket_path,
							   "",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);
	MarkGUCPrefixReserved("pg_raft");

	if (socket_path == NULL || socket_path[0] == '\0')
	{
		ereport(LOG, (errmsg("pg_raft_wal: no pg_raft.socket: the WAL is not replicated")));
		return;
	}
	wal_write_hook = pg_raft_wal_write;
	wal_flush_hook = pg_raft_wal_flush;
	wal_timeline_hook = pg_raft_wal_timeline;
}
