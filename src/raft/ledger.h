#pragma once

#include <deque>

#include "src/base/base.h"
#include "src/raft/entry.h"
#include "src/raft/raft_state.h"

// The ledger: the raft log and the WAL in one sequence of records, in fixed-size files
// (docs/ledger.md).
//
// A record:
//
//   offset 0   u32 magic       kLedgerMagic ("LDGR"); 0: no record here (the end of the log)
//          4   u16 version     kLedgerVersion
//          6   u16 flags       0
//          8   u32 meta_len    the size of the serialized metadata
//         12   u64 idx         the raft index
//         20   u64 term        the raft term
//         28   u32 prev_crc    the crc of the record at idx - 1 (the chain)
//         32   u32 crc         CRC-32C of bytes [0, 32), the metadata and the WAL bytes
//         36   metadata        RawOArchive of {time, WalCommand}
//              WAL bytes       wal_bytes(cmd)->second bytes (none for a command without any)
//
// All integers little-endian. The WAL's length is the WalCommand's (wal_bytes()). A record is
// created once, by the leader that appends it, and copied byte for byte from then on, so the
// bytes of (idx, term) are the same on every node and prev_crc links one chain.
//
// Files: <first index, 16 hex digits>.ledger, each exactly file_size bytes, zero-filled ahead of
// time. A file starts with a file header (FileHeader: the index of its first record, the term and
// crc of the record before it, and the state at that record), then its records. A record that
// doesn't fit in what is left of a file goes to the next file; the file gets an end marker
// (kLedgerEndMagic, then the crc of its last record) if at least kLedgerMarkerSize bytes are left.
// Fewer than that left: the file is full too.
//
// A file is self-contained: an InstallSnapshot is the leader's first file, bytes [0, its written
// offset), and the follower builds everything else from it as it does at startup.

constexpr uint32_t kLedgerMagic      = 0x5247444CU;  // "LDGR"
constexpr uint32_t kLedgerEndMagic   = 0x444E454CU;  // "LEND"
constexpr uint32_t kLedgerFileMagic  = 0x4647444CU;  // "LDGF"
constexpr uint16_t kLedgerVersion    = 1;
constexpr size_t   kLedgerHeaderSize = 36;
constexpr size_t   kLedgerMarkerSize = 8;
constexpr size_t   kLedgerMaxMeta    = 1U << 20U;  // a TimelineOp's history file, at most

struct RecordHeader {
  uint32_t  magic    = kLedgerMagic;
  uint16_t  version  = kLedgerVersion;
  uint16_t  flags    = 0;
  uint32_t  meta_len = 0;
  RaftIndex idx{0};
  RaftTerm  term{0};
  uint32_t  prev_crc = 0;
  uint32_t  crc      = 0;

  bool operator==(RecordHeader const&) const = default;
};

/// Appends the record of entry |e| at |idx| (with the WAL bytes |wal|, wal_bytes(e.cmd)'s size) to
/// |out|; returns its crc. Asserts that |wal| has the command's size.
uint32_t encode_record(string&                  out,
                       RaftIndex                idx,
                       Entry const&             e,
                       std::span<uint8_t const> wal,
                       uint32_t                 prev_crc);

/// The header at the start of a ledger file: u32 magic (kLedgerFileMagic), u32 size of the
/// payload, u32 CRC-32C of the magic, the size and the payload, then the payload (RawOArchive of
/// this struct).
struct FileHeader {
  RaftIndex first{0};      // the index of the file's first record
  RaftTerm  prev_term{0};  // the term of the record at first - 1 (0: none)
  uint32_t  prev_crc = 0;  // the crc of the record at first - 1 (0: none)
  RaftState state;         // the state at first - 1, of the records only (entry_state())

  bool operator==(FileHeader const&) const = default;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & first;
    ar & prev_term;
    ar & prev_crc;
    ar & state;
  }
};

/// The part of a state that the records determine (RaftState::apply): the node's own fields
/// (needs_backup, first_idx, first_segment) cleared.
RaftState entry_state(RaftState s);

string                                encode_file_header(FileHeader const& h);
Result<std::pair<FileHeader, size_t>> decode_file_header(std::span<uint8_t const> buf);

/// One record decoded from memory.
struct DecodedRecord {
  RecordHeader             header;
  Entry                    entry;
  std::span<uint8_t const> wal;    // into the decoded buffer
  std::span<uint8_t const> bytes;  // the whole record, into the decoded buffer
};

/// Decodes the record at the start of |buf|, checking its magic, version, lengths, metadata and
/// crc. EBADMSG if it is not a whole, valid record.
Result<DecodedRecord> decode_record(std::span<uint8_t const> buf);

/// Decodes every record of |buf| (whole records, back to back), checking each and that their
/// indexes are consecutive, their terms don't decrease and they chain. EBADMSG otherwise.
Result<vector<DecodedRecord>> decode_records(std::span<uint8_t const> buf);

/// An entry read from the ledger.
struct LedgerEntry {
  RaftIndex               idx{0};
  Entry                   entry;
  shared_ptr<Bytes const> wal;  // never null; empty without WAL bytes
  uint32_t                crc = 0;
  // The encoded record, as written: kept for the records this ledger appended itself (append(),
  // the leader's), so that records() sends them from memory while they are in the window. Null
  // for the others (a follower's append_records(), entries read back from the files).
  shared_ptr<string const> record;
};

/// Whole records, as stored: at most two buffers (a run read from the files crosses at most one
/// file boundary; one from the window is a single buffer).
struct RecordBatch {
  RaftIndex                first{0};  // the index of the first record
  RaftIndex                last{0};   // the index of the last record; first - 1 when empty
  inplace_vector<Bytes, 2> buffers;

  size_t count() const { return INT(last) + 1 - INT(first); }
};

/// The leader's file for an InstallSnapshot: its first index and the bytes to send, [0, size).
struct SnapshotFile {
  RaftIndex first{0};
  size_t    size = 0;
};

/// A follower's install file, checked (prepare_install()).
struct InstallInfo {
  FileHeader                  header;
  RaftIndex                   last{0};        // its last record
  std::optional<LogSeqNumber> first_segment;  // of its first record with one
};

/// What a follower's append_records() did: |last| is the last record's index; the records from
/// |first_new| on were written (the earlier ones the ledger had).
struct Appended {
  RaftIndex last{0};
  RaftIndex first_new{0};
};

struct LedgerOptions {
  size_t file_size   = 1ULL << 30U;  // each file's size
  size_t sparse_step = 64U << 10U;   // a sparse index point at least every this many bytes
  bool   sync        = false;        // leader_sync: the sync thread syncs every append at once
  bool   no_sync     = false;        // tests only: nothing is ever synced
  bool   spare       = true;         // prepare the next file (zero-filled) in the background
};

class LedgerReader;

class Ledger {
 public:
  Ledger(std::filesystem::path dir, LedgerOptions options = {});
  virtual ~Ledger();

  Ledger(Ledger const&)            = delete;
  Ledger& operator=(Ledger const&) = delete;

  /// Opens the ledger in |dir| (created if missing). |first_idx| is the saved state's first index
  /// (0: never compacted, the log starts at 1); files wholly before it are deleted. |installing|:
  /// the meta file names an install (finish_install()) that a crash may have interrupted.
  Result<void> open(RaftIndex first_idx = RaftIndex{0},
                    std::optional<RaftIndex> installing = std::nullopt);

  /// Stops the threads and closes the files.
  void close();

  // Writes. One writer at a time (the raft server lock). After a failed write every write fails.

  /// The leader's append of |e| at the next index, with its WAL bytes |wal|. Returns the index.
  Result<RaftIndex> append(Entry const& e, std::span<uint8_t const> wal);

  /// The follower's append of whole records (from the leader's ledger), after the entry at
  /// |prev_idx| with term |prev_term|. nullopt if this ledger has no such entry (log matching);
  /// otherwise the index of the last record. Records it already has (same crc) are skipped; at the
  /// first that differs the ledger is truncated. EBADMSG for invalid records, or a first record
  /// that doesn't chain to the entry at prev_idx.
  Result<std::optional<RaftIndex>> append_records(RaftIndex                prev_idx,
                                                  RaftTerm                 prev_term,
                                                  std::span<uint8_t const> records);

  /// The same, with the records decoded (decode_records()). With |sync| (follower_sync), the
  /// records it writes go to disk in the write itself (RWF_DSYNC: no separate fdatasync, and the
  /// sync thread isn't woken); synced_idx() then covers them if it covered everything before.
  Result<std::optional<Appended>> append_records(RaftIndex                    prev_idx,
                                                 RaftTerm                     prev_term,
                                                 vector<DecodedRecord> const& records,
                                                 bool                         sync = false);

  /// Deletes every entry after |idx| (idx >= the first index - 1).
  ///
  /// The files and their records stay readable without the lock below their written offsets: a
  /// truncation lowers a written offset first (it drops only uncommitted records, which a reader
  /// outside the writer doesn't read).
  Result<void> truncate_after(RaftIndex idx);

  /// Syncs the last file now; the durable index then covers every record appended before.
  Result<void> sync();

  /// The highest index known to be on disk (with sync off: the last appended).
  RaftIndex durable_idx() const { return durable_.load(); }

  /// The highest index a sync has put on disk, whatever the options: by sync(), the sync thread,
  /// or an append_records() with |sync|. (On open and after an install: the last entry.)
  RaftIndex synced_idx() const { return synced_.load(); }

  /// Called by the sync thread (without the ledger's locks) after each sync, with durable_idx().
  void set_durable_callback(std::function<void(RaftIndex)> cb);

  // Reads.

  /// The term and index of the last entry; (0, first - 1) when empty.
  tuple<RaftTerm, RaftIndex> last_entry() const;

  /// The index of the first entry (the next one when empty).
  RaftIndex first_idx() const;

  /// The state at the entry before the first one (the first file's header).
  RaftState base_state() const;

  /// The term of the entry at |idx| (the one before the first entry too); nullopt outside.
  std::optional<RaftTerm> term(RaftIndex idx) const;

  /// The crc of the record at |idx|; for first_idx() - 1, the first record's prev_crc.
  Result<std::optional<uint32_t>> crc(RaftIndex idx) const;

  /// At most |count| entries from |idx|, and at most |max_bytes| of records (but at least one);
  /// nullopt if |idx| is before the first entry.
  Result<std::optional<vector<LedgerEntry>>> entries(RaftIndex idx,
                                                     size_t    count,
                                                     size_t    max_bytes = 8U << 20U) const;

  /// The records themselves, for sending: as entries(), at most two files. The leader's latest
  /// records (in the window with their bytes, LedgerEntry::record) come from memory instead, in
  /// one buffer and not limited to two files.
  ///
  /// The reads don't hold the ledger's lock (only to find where to read): a file is read before
  /// its written offset, which the writer publishes after writing the bytes before it.
  Result<std::optional<RecordBatch>> records(RaftIndex idx,
                                             size_t    count,
                                             size_t    max_bytes = 8U << 20U) const;

  /// Checks every record of every file (crc, indexes, terms, chain, markers). EBADMSG, logged.
  Result<void> verify() const;

  /// The entries after |idx| are kept in memory, with their WAL bytes (the uncommitted ones);
  /// release() drops those up to |idx| (applied).
  void release(RaftIndex idx);

  // Compaction: whole files.

  /// The first index of the file holding |idx| (the first index a compaction to |idx| keeps).
  RaftIndex compaction_point(RaftIndex idx) const;

  /// Deletes the files before the one starting at |first|. open() is given the same first index
  /// after a restart.
  Result<void> drop_before(RaftIndex first);

  // InstallSnapshot (docs/ledger.md §6.6). The leader: snapshot_file(), then read_snapshot() for
  // each chunk. The follower: install_chunk() for each, prepare_install(), then the meta file names
  // the install, then finish_install().

  /// The first file and the end of its records; nullopt when the ledger is empty.
  std::optional<SnapshotFile> snapshot_file() const;

  /// The bytes [offset, offset + size) of the file starting at |first| (below its written offset);
  /// nullopt if it is gone (compacted: a snapshot starts again).
  Result<std::optional<Bytes>> read_snapshot(RaftIndex first, size_t offset, size_t size) const;

  /// Writes a chunk of the leader's file into the install file: at offset 0 a new one, otherwise
  /// right after the previous chunk (EINVAL if not).
  Result<void> install_chunk(size_t offset, std::span<uint8_t const> bytes);

  /// Checks the install file whole (its header, every record's crc, the chain, nothing after the
  /// last) and syncs it.
  Result<InstallInfo> prepare_install();

  /// Replaces every file with the install file.
  Result<void> finish_install(RaftIndex first);

  std::filesystem::path const& dir() const { return dir_; }
  size_t                       file_count() const;
  static string                file_name(RaftIndex first);

 protected:
  // The system calls that write, for fault injection.
  virtual ssize_t do_pwrite(int fd, void const* data, size_t size, off_t offset);
  /// A pwrite whose bytes are on disk when it returns (Linux: pwritev2 with RWF_DSYNC).
  virtual ssize_t do_pwrite_dsync(int fd, void const* data, size_t size, off_t offset);
  virtual int     do_datasync(int fd);
  virtual int     do_rename(char const* from, char const* to);

 private:
  struct File;
  struct Pending;

  Result<void> check_writable() const;
  Result<void> fail(std::error_code ec);

  Result<void> load_files(RaftIndex first_idx);

  Result<void> write_records(vector<Pending> const& records, bool dsync = false);
  Result<void> rotate(RaftIndex idx);
  Result<shared_ptr<File>> create_file(FileHeader const& header);
  Result<void> pwrite_all(int fd, void const* data, size_t size, off_t offset, bool dsync = false);
  Result<void> zero_range(int fd, size_t from, size_t to);
  Result<void> fsync_dir();

  Result<void> truncate_locked(RaftIndex idx);

  /// The state after the records of |f| up to |last| (from its header).
  Result<RaftState> fold(File const& f, size_t limit) const;

  /// A place to read from: a file and a record in it, with the file's last index and written
  /// offset, taken under the lock; the reads from it don't need the lock.
  struct Cursor {
    shared_ptr<File> f;
    RaftIndex        at{0};
    size_t           off   = 0;
    RaftIndex        last  {0};
    size_t           limit = 0;
  };

  /// A sparse point at or before |idx| in the file holding it; the start of the next file.
  std::optional<Cursor> cursor_locked(RaftIndex idx) const;
  std::optional<Cursor> next_cursor_locked(Cursor const& c) const;

  /// Moves the cursor to the record |idx| (reads, no lock).
  Result<void> seek(Cursor& c, RaftIndex idx) const;
  Result<void> seek(Cursor& c, LedgerReader& r, RaftIndex idx) const;

  /// The file holding |idx|, and the offset of its record (the writer's lock held).
  Result<std::optional<std::pair<shared_ptr<File>, size_t>>> locate(RaftIndex idx) const;

  Result<uint32_t>                         crc_at(Cursor c, RaftIndex idx) const;
  std::optional<std::optional<uint32_t>>   crc_in_memory(RaftIndex idx) const;
  Result<std::optional<uint32_t>>          crc_locked(RaftIndex idx) const;
  std::optional<RaftTerm>            term_locked(RaftIndex idx) const;
  RaftIndex                          last_locked() const;
  RaftTerm                           last_term_locked() const;

  void sync_loop();
  void spare_loop();
  Result<void> sync_once();
  bool         take_spare(std::filesystem::path const& to);

  std::filesystem::path const dir_;
  LedgerOptions const         options_;

  mutable std::shared_mutex          mtx_;  // files_, the tail, terms_, window_
  map<RaftIndex, shared_ptr<File>>   files_;
  map<RaftIndex, RaftTerm>           terms_;   // the term from each index on
  RaftIndex                          base_idx_{0};  // the index before the first entry
  uint32_t                           base_crc_ = 0;  // the crc of the entry at base_idx_
  RaftTerm                           base_term_{0};  // the term of the entry at base_idx_
  RaftState                          base_state_;    // the state at base_idx_
  RaftState                          tail_state_;    // the state at the last entry (entry_state)
  size_t                             install_size_ = 0;  // of the install file so far
  std::deque<LedgerEntry>            window_;
  RaftIndex                          released_{0};
  std::error_code                    failed_;
  uint64_t                           generation_ = 0;  // bumped by every truncation

  std::atomic<RaftIndex> durable_{RaftIndex{0}};
  std::atomic<RaftIndex> synced_{RaftIndex{0}};  // written under mtx_

  std::mutex                     sync_mtx_;
  std::condition_variable        sync_cv_;
  bool                           sync_stop_ = false;
  bool                           sync_wake_ = false;
  std::function<void(RaftIndex)> durable_cb_;
  std::thread                    sync_thread_;

  std::mutex              spare_mtx_;
  std::condition_variable spare_cv_;
  bool                    spare_stop_  = false;
  bool                    spare_ready_ = false;
  std::thread             spare_thread_;
};

// The term file and the meta file: small files written whole (a temporary file, synced, renamed
// over the old one, the directory synced), each with a CRC-32C of its contents.

struct TermVote {
  RaftTerm                  term{0};
  std::optional<RaftNodeId> voted_for;

  bool operator==(TermVote const&) const = default;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & term;
    ar & voted_for;
  }
};

/// The term and vote (the term file): the term only moves forward, a new
/// term clears the vote, and a node votes once per term.
class TermFile {
 public:
  TermFile(std::filesystem::path path, bool no_sync) : path_(std::move(path)), no_sync_(no_sync) {}

  /// Reads the file (TermVote{} if missing).
  Result<TermVote> open();

  TermVote get() const;

  /// Moves the term to |term| (clearing the vote) if it is newer; returns the current term.
  Result<RaftTerm> set_current_term(RaftTerm term);

  /// Votes for |node| in |term|, the current term: true if this is the vote (now or before).
  Result<bool> vote(RaftTerm term, RaftNodeId node);

 private:
  std::filesystem::path const path_;
  bool const                  no_sync_;
  mutable std::mutex          mtx_;
  TermVote                    tv_;
};

struct LedgerMeta {
  RaftState                state;  // the applied state (a checkpoint)
  std::optional<RaftIndex> installing;

  bool operator==(LedgerMeta const&) const = default;

  template <class Archive>
  void serialize(Archive& ar, unsigned int const /*version*/) {
    ar & state;
    ar & installing;
  }
};

/// Writes |contents| to |path| whole. No sync with |no_sync| (tests).
Result<void> write_checked_file(std::filesystem::path const& path,
                                string const&                contents,
                                bool                         no_sync = false);

/// The contents of a file written by write_checked_file(); ENOENT if missing, EBADMSG if its
/// checksum or size doesn't match.
Result<string> read_checked_file(std::filesystem::path const& path);

Result<void>     write_term_file(std::filesystem::path const& path, TermVote const& t, bool no_sync);
Result<TermVote> read_term_file(std::filesystem::path const& path);  // TermVote{} if missing

Result<void> write_meta_file(std::filesystem::path const& path, LedgerMeta const& m, bool no_sync);
Result<LedgerMeta> read_meta_file(std::filesystem::path const& path);  // LedgerMeta{} if missing
