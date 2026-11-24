#include "src/raft/ledger.h"

#include <sys/uio.h>

#include <charconv>

#include "src/base/archive.h"
#include "src/base/crc32c.h"
#include "src/base/logger.h"

namespace fs = std::filesystem;

namespace {

constexpr size_t   kChunk          = 1U << 20U;  // zero writes
constexpr size_t   kReadChunk      = 64U << 10U;  // reads
constexpr uint32_t kCheckedMagic   = 0x4C464B43U;  // "CKFL"
constexpr size_t   kCheckedHeader  = 16;           // magic, crc, size (u64)
constexpr char     kSpareName[]    = ".spare.ledger";
constexpr char     kSpareTmpName[] = ".spare.tmp";
constexpr char     kInstallName[]  = ".install.ledger";
constexpr char     kExtension[]    = ".ledger";

template <typename T>
void put_le(uint8_t* p, T v) {
  v = boost::endian::native_to_little(v);
  std::memcpy(p, &v, sizeof(v));
}

template <typename T>
T get_le(uint8_t const* p) {
  T v;
  std::memcpy(&v, p, sizeof(v));
  return boost::endian::little_to_native(v);
}

void encode_header(uint8_t* p, RecordHeader const& h) {
  put_le<uint32_t>(p, h.magic);
  put_le<uint16_t>(p + 4, h.version);
  put_le<uint16_t>(p + 6, h.flags);
  put_le<uint32_t>(p + 8, h.meta_len);
  put_le<uint64_t>(p + 12, INT(h.idx));
  put_le<uint64_t>(p + 20, INT(h.term));
  put_le<uint32_t>(p + 28, h.prev_crc);
  put_le<uint32_t>(p + 32, h.crc);
}

/// The header at |p| (kLedgerHeaderSize bytes), if it is one: magic, version, flags, meta_len.
std::optional<RecordHeader> parse_header(uint8_t const* p) {
  RecordHeader h{
      .magic    = get_le<uint32_t>(p),
      .version  = get_le<uint16_t>(p + 4),
      .flags    = get_le<uint16_t>(p + 6),
      .meta_len = get_le<uint32_t>(p + 8),
      .idx      = RaftIndex{get_le<uint64_t>(p + 12)},
      .term     = RaftTerm{get_le<uint64_t>(p + 20)},
      .prev_crc = get_le<uint32_t>(p + 28),
      .crc      = get_le<uint32_t>(p + 32),
  };
  if (h.magic != kLedgerMagic || h.version != kLedgerVersion || h.flags != 0 ||
      h.meta_len > kLedgerMaxMeta)
    return std::nullopt;
  return h;
}

struct Meta {
  std::time_t time{0};
  WalCommand  cmd;
};

std::optional<Meta> decode_meta(uint8_t const* p, size_t size) {
  try {
    Meta        m;
    RawIArchive ia(p, size);
    ia >> m.time;
    ia >> m.cmd;
    return m;
  } catch (std::exception const&) {
    return std::nullopt;
  }
}

size_t wal_size(WalCommand const& cmd) {
  auto b = wal_bytes(cmd);
  return b ? b->second : 0U;
}

/// The crc of a record: bytes [0, 32) of the header, then everything after the header.
uint32_t record_crc(uint8_t const* p, size_t size) {
  auto crc = crc32c(0, p, kLedgerHeaderSize - 4U);
  return crc32c(crc, p + kLedgerHeaderSize, size - kLedgerHeaderSize);
}

int datasync(int fd) {
#ifdef __APPLE__
  return ::fsync(fd);
#else
  return ::fdatasync(fd);
#endif
}

std::optional<RaftIndex> parse_file_name(string const& name) {
  constexpr size_t kDigits = 16;
  if (name.size() != kDigits + std::strlen(kExtension) || !name.ends_with(kExtension))
    return std::nullopt;
  uint64_t v = 0;
  auto [ptr, ec] = std::from_chars(name.data(), name.data() + kDigits, v, 16);
  if (ec != std::errc{} || ptr != name.data() + kDigits)
    return std::nullopt;
  return RaftIndex{v};
}

}  // namespace

/// Buffered reads of a file, of the bytes before |limit| (the written offset) only: a read that
/// reaches it comes back short, and one at or past it empty, whatever the file holds there. The scan
/// (next_slot) takes that for the end of the log, as it would a zero magic.
class LedgerReader {
 public:
  LedgerReader(int fd, size_t file_size, size_t limit)
      : fd_(fd), limit_(std::min(limit, file_size)) {}

  /// The bytes [off, off + n) below the limit: fewer (or none) when they reach it.
  Result<std::span<uint8_t const>> get(size_t off, size_t n) {
    if (off >= limit_)
      return std::span<uint8_t const>{};
    n = std::min(n, limit_ - off);
    if (off >= off_ && off + n <= off_ + len_)
      return std::span<uint8_t const>(buf_.get() + (off - off_), n);
    auto const want = std::min(std::max(n, kReadChunk), limit_ - off);
    if (cap_ < want) {
      buf_ = std::make_unique_for_overwrite<uint8_t[]>(want);
      cap_ = want;
    }
    size_t got = 0;
    while (got < want) {
      auto r = ::pread(fd_, buf_.get() + got, want - got, static_cast<off_t>(off + got));
      if (r < 0) {
        if (errno == EINTR)
          continue;
        len_ = 0;
        return system_error_code(errno);
      }
      if (r == 0)
        break;  // a short file
      got += static_cast<size_t>(r);
    }
    off_ = off;
    len_ = got;
    return std::span<uint8_t const>(buf_.get(), std::min(n, got));
  }

 private:
  int                        fd_;
  size_t                     limit_;
  size_t                     off_ = 0;
  size_t                     len_ = 0;  // the bytes of buf_ read, from off_
  size_t                     cap_ = 0;
  std::unique_ptr<uint8_t[]> buf_;
};

namespace {

enum class Slot { kRecord, kEnd, kMarker, kFull, kBad };

struct Next {
  Slot         slot = Slot::kBad;
  RecordHeader h;
  Entry        e{};
  size_t       size       = 0;  // of the record
  uint32_t     marker_crc = 0;
};

/// What is at |off|. With |verify| the whole record is read and its crc checked.
Result<Next> next_slot(LedgerReader& r, size_t off, size_t file_size, bool verify) {
  auto const rem = file_size - off;
  if (rem < kLedgerMarkerSize)
    return Next{.slot = Slot::kFull};
  auto b = r.get(off, kLedgerMarkerSize);
  if (!b)
    return b.error();
  if (b->size() < kLedgerMarkerSize)
    return Next{.slot = Slot::kEnd};  // at the written offset (or a short file): the end
  auto const magic = get_le<uint32_t>(b->data());
  if (magic == 0)
    return Next{.slot = Slot::kEnd};
  if (magic == kLedgerEndMagic)
    return Next{.slot = Slot::kMarker, .marker_crc = get_le<uint32_t>(b->data() + 4)};
  if (magic != kLedgerMagic || rem < kLedgerHeaderSize)
    return Next{};

  auto hb = r.get(off, kLedgerHeaderSize);
  if (!hb)
    return hb.error();
  if (hb->size() < kLedgerHeaderSize)
    return Next{};
  auto h = parse_header(hb->data());
  if (!h || kLedgerHeaderSize + h->meta_len > rem)
    return Next{};
  auto mb = r.get(off + kLedgerHeaderSize, h->meta_len);
  if (!mb)
    return mb.error();
  if (mb->size() < h->meta_len)
    return Next{};
  auto m = decode_meta(mb->data(), mb->size());
  if (!m)
    return Next{};
  auto const total = kLedgerHeaderSize + h->meta_len + wal_size(m->cmd);
  if (total > rem)
    return Next{};
  if (verify) {
    auto all = r.get(off, total);
    if (!all)
      return all.error();
    if (all->size() < total || record_crc(all->data(), total) != h->crc)
      return Next{};
  }
  return Next{
      .slot = Slot::kRecord,
      .h    = *h,
      .e    = Entry{.time = m->time, .term = h->term, .cmd = std::move(m->cmd)},
      .size = total,
  };
}

enum class Stop { kEnd, kMarker, kFull, kBad };

struct ScanResult {
  Stop      stop = Stop::kEnd;
  size_t    end  = 0;
  RaftIndex last{0};
  uint32_t  last_crc = 0;
  RaftTerm  last_term{0};
  size_t    bad_at = 0;  // the offset where a kBad stop found what it did
};

using RecordFn = std::function<void(RecordHeader const&, Entry const&, size_t off)>;

/// Walks the records of a file from its start. |first|: the index of the first record; |chain|:
/// the crc of the record before it (nullopt: not checked); |min_term|: the term before it.
Result<ScanResult> scan_file(int                     fd,
                             size_t                  file_size,
                             size_t                  limit,
                             size_t                  start,
                             RaftIndex               first,
                             bool                    verify,
                             std::optional<uint32_t> chain,
                             RaftTerm                min_term,
                             RecordFn const&         fn) {
  LedgerReader     r(fd, file_size, limit);
  ScanResult res{
      .end = start, .last = first - 1U, .last_crc = chain.value_or(0U), .last_term = min_term};
  size_t off = start;
  for (;;) {
    auto n = next_slot(r, off, file_size, verify);
    if (!n)
      return n.error();
    res.end = off;
    switch (n->slot) {
      case Slot::kEnd:
        res.stop = Stop::kEnd;
        return res;
      case Slot::kFull:
        res.stop = Stop::kFull;
        return res;
      case Slot::kMarker:
        if (res.last >= first && n->marker_crc == res.last_crc) {
          res.stop = Stop::kMarker;
          return res;
        }
        res.stop   = Stop::kBad;
        res.bad_at = off;
        return res;
      case Slot::kBad:
        res.stop   = Stop::kBad;
        res.bad_at = off;
        return res;
      case Slot::kRecord:
        break;
    }
    auto const& h = n->h;
    if (h.idx != res.last + 1U || h.term < res.last_term || (chain && h.prev_crc != *chain)) {
      res.stop   = Stop::kBad;
      res.bad_at = off;
      return res;
    }
    if (fn)
      fn(h, n->e, off);
    chain         = h.crc;
    res.last      = h.idx;
    res.last_crc  = h.crc;
    res.last_term = h.term;
    off += n->size;
  }
}

Result<void> fsync_path(fs::path const& dir) {
  int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd == -1)
    return system_error_code(errno);
  int rc  = ::fsync(fd);
  int err = errno;
  ::close(fd);
  if (rc != 0)
    return system_error_code(err);
  return ok();
}

}  // namespace

// Records.

uint32_t encode_record(string&                  out,
                       RaftIndex                idx,
                       Entry const&             e,
                       std::span<uint8_t const> wal,
                       uint32_t                 prev_crc) {
  assert(wal.size() == wal_size(e.cmd));
  auto const start = out.size();
  out.resize(start + kLedgerHeaderSize);
  {
    RawOArchive oa(out);
    oa << e.time;
    oa << e.cmd;
  }
  auto const meta_len = out.size() - start - kLedgerHeaderSize;
  if (!wal.empty())
    out.append(reinterpret_cast<char const*>(wal.data()), wal.size());

  auto* p = reinterpret_cast<uint8_t*>(out.data()) + start;
  RecordHeader h{
      .meta_len = u4(meta_len),
      .idx      = idx,
      .term     = e.term,
      .prev_crc = prev_crc,
  };
  encode_header(p, h);
  auto const crc = record_crc(p, out.size() - start);
  put_le<uint32_t>(p + 32, crc);
  return crc;
}

Result<DecodedRecord> decode_record(std::span<uint8_t const> buf) {
  if (buf.size() < kLedgerHeaderSize)
    return system_error_code(EBADMSG);
  auto h = parse_header(buf.data());
  if (!h || buf.size() < kLedgerHeaderSize + h->meta_len)
    return system_error_code(EBADMSG);
  auto m = decode_meta(buf.data() + kLedgerHeaderSize, h->meta_len);
  if (!m)
    return system_error_code(EBADMSG);
  auto const meta_end = kLedgerHeaderSize + h->meta_len;
  auto const ws       = wal_size(m->cmd);
  if (buf.size() - meta_end < ws)
    return system_error_code(EBADMSG);
  auto const total = meta_end + ws;
  if (record_crc(buf.data(), total) != h->crc)
    return system_error_code(EBADMSG);
  return DecodedRecord{
      .header = *h,
      .entry  = Entry{.time = m->time, .term = h->term, .cmd = std::move(m->cmd)},
      .wal    = buf.subspan(meta_end, ws),
      .bytes  = buf.subspan(0, total),
  };
}

Result<vector<DecodedRecord>> decode_records(std::span<uint8_t const> buf) {
  vector<DecodedRecord> out;
  while (!buf.empty()) {
    auto r = decode_record(buf);
    if (!r)
      return r.error();
    if (!out.empty()) {
      auto const& prev = out.back().header;
      if (r->header.idx != prev.idx + 1U || r->header.term < prev.term ||
          r->header.prev_crc != prev.crc)
        return system_error_code(EBADMSG);
    }
    buf = buf.subspan(r->bytes.size());
    out.push_back(std::move(*r));
  }
  return out;
}

RaftState entry_state(RaftState s) {
  s.needs_backup  = false;
  s.first_idx     = RaftIndex{0};
  s.first_segment = LogSeqNumber{};
  return s;
}

string encode_file_header(FileHeader const& h) {
  string payload;
  {
    RawOArchive oa(payload);
    oa << h;
  }
  string out(12, '\0');
  auto*  p = reinterpret_cast<uint8_t*>(out.data());
  put_le<uint32_t>(p, kLedgerFileMagic);
  put_le<uint32_t>(p + 4, u4(payload.size()));
  auto crc = crc32c(0, p, 8);
  crc      = crc32c(crc, payload.data(), payload.size());
  put_le<uint32_t>(p + 8, crc);
  return out + payload;
}

Result<std::pair<FileHeader, size_t>> decode_file_header(std::span<uint8_t const> buf) {
  if (buf.size() < 12 || get_le<uint32_t>(buf.data()) != kLedgerFileMagic)
    return system_error_code(EBADMSG);
  auto const len = get_le<uint32_t>(buf.data() + 4);
  if (len > kLedgerMaxMeta || buf.size() - 12 < len)
    return system_error_code(EBADMSG);
  auto crc = crc32c(0, buf.data(), 8);
  crc      = crc32c(crc, buf.data() + 12, len);
  if (crc != get_le<uint32_t>(buf.data() + 8))
    return system_error_code(EBADMSG);
  try {
    FileHeader  h;
    RawIArchive ia(buf.data() + 12, len);
    ia >> h;
    if (h.first == RaftIndex{0} || h.state.log_idx + 1U != h.first)
      return system_error_code(EBADMSG);
    return std::pair{h, size_t{12} + len};
  } catch (std::exception const&) {
    return system_error_code(EBADMSG);
  }
}

namespace {

/// The header of the file |fd| (of |file_size|), read below |limit|.
Result<std::pair<FileHeader, size_t>> read_file_header(int fd, size_t file_size, size_t limit) {
  LedgerReader r(fd, file_size, limit);
  auto   b = r.get(0, std::min(file_size, kLedgerMaxMeta + 12U));
  if (!b)
    return b.error();
  return decode_file_header(*b);
}

}  // namespace

// The ledger.

struct Ledger::File {
  fs::path  path;
  int       fd = -1;
  RaftIndex  first{0};
  RaftIndex  last{0};       // first - 1 when empty
  FileHeader header;        // the file header
  size_t     start    = 0;  // the offset of the first record (the header's size)
  size_t     end      = 0;  // the offset after the last record
  uint32_t  last_crc = 0;  // the crc of the last record (empty: of the record before first)
  bool      full     = false;

  /// The written offset, published after the bytes before it are written (end; the file size
  /// once the file is full): a reader reads only before it, without the ledger's lock.
  std::atomic<unsigned> written{0};

  vector<std::pair<RaftIndex, size_t>> sparse;  // (index, offset); the first record at 0

  void publish() { written.store(u4(end), std::memory_order_release); }
  size_t size = 0;  // the file size

  File() = default;
  File(File const&)            = delete;
  File& operator=(File const&) = delete;
  ~File() {
    if (fd >= 0)
      ::close(fd);
  }
};

struct Ledger::Pending {
  std::span<uint8_t const> bytes;
  LedgerEntry              entry;
};

Ledger::Ledger(fs::path dir, LedgerOptions options)
    : dir_(std::move(dir)), options_(options) {}

Ledger::~Ledger() {
  close();
}

string Ledger::file_name(RaftIndex first) {
  return std::format("{:016X}{}", INT(first), kExtension);
}

ssize_t Ledger::do_pwrite(int fd, void const* data, size_t size, off_t offset) {
  return ::pwrite(fd, data, size, offset);
}

ssize_t Ledger::do_pwrite_dsync(int fd, void const* data, size_t size, off_t offset) {
#ifdef __APPLE__
  auto n = ::pwrite(fd, data, size, offset);
  if (n > 0 && ::fsync(fd) != 0)
    return -1;
  return n;
#else
  iovec iov{.iov_base = const_cast<void*>(data), .iov_len = size};
  return ::pwritev2(fd, &iov, 1, offset, RWF_DSYNC);
#endif
}

int Ledger::do_datasync(int fd) {
  return datasync(fd);
}

int Ledger::do_rename(char const* from, char const* to) {
  return ::rename(from, to);
}

Result<void> Ledger::open(RaftIndex first_idx, std::optional<RaftIndex> installing) {
  std::error_code ec;
  fs::create_directories(dir_, ec);
  if (ec)
    return elog(ec, "Unable to create {}", dir_);
  fs::remove(dir_ / kSpareTmpName, ec);

  auto install = dir_ / kInstallName;
  if (installing && fs::exists(install)) {
    log(INFO, "ledger {}: finishing the install of {}", dir_, *installing);
    for (auto const& entry : fs::directory_iterator(dir_)) {
      if (parse_file_name(entry.path().filename().string()) && ::unlink(entry.path().c_str()) != 0)
        return elog(system_error_code(errno), "Unable to delete {}", entry.path());
    }
    auto to = dir_ / file_name(*installing);
    if (do_rename(install.c_str(), to.c_str()) != 0)
      return elog(system_error_code(errno), "Unable to rename {} to {}", install, to);
    if (!options_.no_sync)
      if (auto r = fsync_path(dir_); !r)
        return elog(r.error(), "Unable to sync {}", dir_);
  } else {
    fs::remove(install, ec);
  }

  {
    auto lk = std::unique_lock{mtx_};
    if (auto r = load_files(first_idx); !r)
      return r;
    released_ = last_locked();
    durable_.store(released_);
    synced_.store(released_);
  }

  {
    auto lk      = std::unique_lock{spare_mtx_};
    auto spare   = dir_ / kSpareName;
    spare_ready_ = fs::exists(spare, ec) && fs::file_size(spare, ec) == options_.file_size;
    spare_stop_  = false;
  }
  if (options_.spare)
    spare_thread_ = std::thread([this] { spare_loop(); });

  sync_stop_ = false;
  if (!options_.no_sync)
    sync_thread_ = std::thread([this] { sync_loop(); });
  return ok();
}

void Ledger::close() {
  {
    auto lk    = std::unique_lock{sync_mtx_};
    sync_stop_ = true;
  }
  sync_cv_.notify_all();
  if (sync_thread_.joinable())
    sync_thread_.join();

  {
    auto lk     = std::unique_lock{spare_mtx_};
    spare_stop_ = true;
  }
  spare_cv_.notify_all();
  if (spare_thread_.joinable())
    spare_thread_.join();

  auto lk = std::unique_lock{mtx_};
  files_.clear();
  terms_.clear();
  window_.clear();
}

Result<void> Ledger::load_files(RaftIndex first_idx) {
  files_.clear();
  terms_.clear();
  window_.clear();

  vector<RaftIndex> firsts;
  for (auto const& entry : fs::directory_iterator(dir_)) {
    if (auto first = parse_file_name(entry.path().filename().string()); first)
      firsts.push_back(*first);
  }
  std::ranges::sort(firsts);

  // The files before first_idx's (a compaction that saved its state and crashed before deleting
  // them all).
  while (firsts.size() >= 2 && firsts.at(1) <= first_idx) {
    auto path = dir_ / file_name(firsts.front());
    log(INFO, "ledger {}: deleting {}, before the first index {}", dir_, path, first_idx);
    if (::unlink(path.c_str()) != 0)
      return elog(system_error_code(errno), "Unable to delete {}", path);
    firsts.erase(firsts.begin());
  }

  base_idx_   = RaftIndex{0};
  base_crc_   = 0;
  base_term_  = RaftTerm{0};
  base_state_ = RaftState{};
  tail_state_ = RaftState{};

  for (size_t i = 0; i < firsts.size(); ++i) {
    auto f   = make_shared<File>();
    f->first = firsts.at(i);
    f->size  = options_.file_size;
    f->path  = dir_ / file_name(f->first);
    f->fd    = ::open(f->path.c_str(), O_RDWR);
    if (f->fd == -1)
      return elog(system_error_code(errno), "Unable to open {}", f->path);
    bool const is_last = i + 1 == firsts.size();

    auto h = read_file_header(f->fd, options_.file_size, options_.file_size);
    if (!h || h->first.first != f->first) {
      if (!is_last)
        return elog(system_error_code(EBADMSG), "ledger {}: {} has no valid header", dir_, f->path);
      // A file created by a rotation that a crash cut short: it has no record either.
      log(WARN, "ledger {}: deleting {}, which has no valid header", dir_, f->path);
      if (::unlink(f->path.c_str()) != 0)
        return elog(system_error_code(errno), "Unable to delete {}", f->path);
      break;
    }
    f->header = h->first;
    f->start  = h->second;

    if (i == 0) {
      base_idx_   = f->first - 1U;
      base_crc_   = f->header.prev_crc;
      base_term_  = f->header.prev_term;
      base_state_ = f->header.state;
      tail_state_ = f->header.state;
    } else {
      // It continues the previous file: the chain, the term, and the state.
      auto const& prev = *files_.rbegin()->second;
      if (f->first != prev.last + 1U || f->header.prev_crc != prev.last_crc ||
          f->header.prev_term != last_term_locked() ||
          !(f->header.state == entry_state(tail_state_)))
        return elog(system_error_code(EBADMSG),
                    "ledger {}: {} does not continue index {}",
                    dir_,
                    f->path,
                    prev.last);
    }

    auto const step = options_.sparse_step;
    auto       res  = scan_file(f->fd,
                         options_.file_size,
                         options_.file_size,
                         f->start,
                         f->first,
                         /*verify=*/is_last,
                         f->header.prev_crc,
                         f->header.prev_term,
                         [&](RecordHeader const& rh, Entry const& e, size_t off) {
                           if (f->sparse.empty() || off >= f->sparse.back().second + step)
                             f->sparse.emplace_back(rh.idx, off);
                           if (terms_.empty() || terms_.rbegin()->second != rh.term)
                             terms_[rh.idx] = rh.term;
                           tail_state_.apply(rh.idx, e);
                         });
    if (!res)
      return elog(res.error(), "Unable to read {}", f->path);

    if (!is_last && (res->last < f->first || (res->stop != Stop::kMarker && res->stop != Stop::kFull)))
      return elog(system_error_code(EBADMSG),
                  "ledger {}: {} is incomplete: it ends at index {}, offset {}",
                  dir_,
                  f->path,
                  res->last,
                  res->end);
    if (res->stop == Stop::kBad)
      log(WARN,
          "ledger {}: the log ends at index {} ({}, offset {}): no valid record after it",
          dir_,
          res->last,
          f->path,
          res->bad_at);

    f->last     = res->last;
    f->end      = res->end;
    f->last_crc = res->last_crc;
    f->full     = res->stop == Stop::kMarker || res->stop == Stop::kFull;
    f->publish();
    files_.emplace(f->first, std::move(f));
  }

  if (files_.empty() && first_idx > RaftIndex{1})
    return elog(system_error_code(EBADMSG),
                "ledger {}: no file, and the saved state starts at {}",
                dir_,
                first_idx);
  return ok();
}

// Writes.

Result<void> Ledger::check_writable() const {
  if (failed_)
    return failed_;
  return ok();
}

Result<void> Ledger::fail(std::error_code ec) {
  if (!failed_)
    failed_ = ec;
  return ec;
}

Result<void> Ledger::pwrite_all(int fd, void const* data, size_t size, off_t offset, bool dsync) {
  auto const* p = static_cast<uint8_t const*>(data);
  while (size > 0) {
    auto n = dsync ? do_pwrite_dsync(fd, p, size, offset) : do_pwrite(fd, p, size, offset);
    if (n < 0) {
      if (errno == EINTR)
        continue;
      return system_error_code(errno);
    }
    if (n == 0)
      return system_error_code(EIO);
    p += n;
    size -= static_cast<size_t>(n);
    offset += n;
  }
  return ok();
}

Result<void> Ledger::zero_range(int fd, size_t from, size_t to) {
  static Bytes const zeros(kChunk, 0);
  while (from < to) {
    auto n = std::min(kChunk, to - from);
    if (auto r = pwrite_all(fd, zeros.data(), n, static_cast<off_t>(from)); !r)
      return r;
    from += n;
  }
  return ok();
}

Result<void> Ledger::fsync_dir() {
  if (options_.no_sync)
    return ok();
  return fsync_path(dir_);
}

bool Ledger::take_spare(fs::path const& to) {
  auto lk = std::unique_lock{spare_mtx_};
  if (!spare_ready_)
    return false;
  auto spare = dir_ / kSpareName;
  if (do_rename(spare.c_str(), to.c_str()) != 0) {
    log(WARN, "ledger {}: unable to rename {} to {}: {}", dir_, spare, to, strerror(errno));
    return false;
  }
  spare_ready_ = false;
  lk.unlock();
  spare_cv_.notify_all();
  return true;
}

Result<shared_ptr<Ledger::File>> Ledger::create_file(FileHeader const& header) {
  auto const first = header.first;
  auto       f     = make_shared<File>();
  f->first         = first;
  f->last          = first - 1U;
  f->size          = options_.file_size;
  f->header        = header;
  f->last_crc      = header.prev_crc;
  f->path          = dir_ / file_name(first);
  if (take_spare(f->path)) {
    f->fd = ::open(f->path.c_str(), O_RDWR);
    if (f->fd == -1)
      return elog(system_error_code(errno), "Unable to open {}", f->path);
  } else {
    if (options_.spare)
      log(WARN, "ledger {}: no spare file ready for {}", dir_, f->path);
    f->fd = ::open(f->path.c_str(), O_RDWR | O_CREAT | O_TRUNC, file_mode());
    if (f->fd == -1)
      return elog(system_error_code(errno), "Unable to create {}", f->path);
    if (::ftruncate(f->fd, static_cast<off_t>(options_.file_size)) != 0)
      return elog(system_error_code(errno), "Unable to size {}", f->path);
  }
  // The header; synced with the file's first records.
  auto const h = encode_file_header(header);
  if (auto r = pwrite_all(f->fd, h.data(), h.size(), 0); !r)
    return elog(r.error(), "Unable to write the header of {}", f->path);
  f->start = f->end = h.size();
  f->publish();
  if (auto r = fsync_dir(); !r)
    return elog(r.error(), "Unable to sync {}", dir_);
  return f;
}

Result<void> Ledger::rotate(RaftIndex idx) {
  // The new file continues the last entry: its crc, its term and the state after it.
  auto const header = FileHeader{
      .first     = idx,
      .prev_term = last_term_locked(),
      .prev_crc  = files_.empty() ? base_crc_ : files_.rbegin()->second->last_crc,
      .state     = entry_state(tail_state_),
  };
  if (!files_.empty()) {
    auto& cur = files_.rbegin()->second;
    if (!cur->full) {
      if (options_.file_size - cur->end >= kLedgerMarkerSize) {
        std::array<uint8_t, kLedgerMarkerSize> marker{};
        put_le<uint32_t>(marker.data(), kLedgerEndMagic);
        put_le<uint32_t>(marker.data() + 4, cur->last_crc);
        if (auto r = pwrite_all(cur->fd, marker.data(), marker.size(), static_cast<off_t>(cur->end));
            !r)
          return fail(elog(r.error(), "Unable to write the end marker of {}", cur->path));
      }
      cur->full = true;
      cur->publish();
    }
    if (!options_.no_sync && do_datasync(cur->fd) != 0)
      return fail(elog(system_error_code(errno), "Unable to sync {}", cur->path));
  }
  auto f = create_file(header);
  if (!f)
    return fail(f.error());
  files_.emplace(idx, std::move(*f));
  return ok();
}

Result<void> Ledger::write_records(vector<Pending> const& records, bool dsync) {
  if (auto r = check_writable(); !r)
    return r;
  dsync = dsync && !options_.no_sync;
  // Synced through the last entry before these records: then they extend synced_.
  bool const contiguous = synced_.load() >= last_locked();

  size_t i = 0;
  while (i < records.size()) {
    auto const size = records.at(i).bytes.size();
    if (files_.empty() || files_.rbegin()->second->full ||
        files_.rbegin()->second->end + size > options_.file_size)
      if (auto r = rotate(records.at(i).entry.idx); !r)
        return r;
    auto& f = files_.rbegin()->second;
    if (f->end + size > options_.file_size)
      return elog(system_error_code(EFBIG), "ledger {}: a record of {} bytes", dir_, size);

    // The run of records that fit in this file, written at once.
    size_t j     = i;
    size_t total = 0;
    while (j < records.size() && f->end + total + records.at(j).bytes.size() <= options_.file_size)
      total += records.at(j++).bytes.size();

    // A file's first records: its header (written by create_file, not synced) is synced with
    // them, so the whole file is synced after the write rather than the write's range.
    bool const first_records = f->end == f->start;
    bool const range_dsync   = dsync && !first_records;

    Result<void> w = ok();
    if (j == i + 1) {
      w = pwrite_all(
          f->fd, records.at(i).bytes.data(), total, static_cast<off_t>(f->end), range_dsync);
    } else {
      Bytes buf;
      buf.reserve(total);
      for (auto k = i; k < j; ++k)
        buf.insert(buf.end(), records.at(k).bytes.begin(), records.at(k).bytes.end());
      w = pwrite_all(f->fd, buf.data(), total, static_cast<off_t>(f->end), range_dsync);
    }
    if (w && dsync && first_records && do_datasync(f->fd) != 0)
      w = system_error_code(errno);
    if (!w)
      return fail(elog(w.error(), "Unable to write {}", f->path));

    for (auto k = i; k < j; ++k) {
      auto const& p = records.at(k);
      if (f->sparse.empty() || f->end >= f->sparse.back().second + options_.sparse_step)
        f->sparse.emplace_back(p.entry.idx, f->end);
      if (terms_.empty() || terms_.rbegin()->second != p.entry.entry.term)
        terms_[p.entry.idx] = p.entry.entry.term;
      tail_state_.apply(p.entry.idx, p.entry.entry);
      f->end += p.bytes.size();
      f->last     = p.entry.idx;
      f->last_crc = p.entry.crc;
      if (p.entry.idx > released_)
        window_.push_back(p.entry);
    }
    if (options_.file_size - f->end < kLedgerMarkerSize)
      f->full = true;  // no room for a marker: full without one
    f->publish();
    i = j;
  }

  // With dsync the records are on disk already (a file rotated in between was synced by rotate()).
  if (dsync && contiguous)
    synced_.store(last_locked());
  if (options_.no_sync || !options_.sync) {
    durable_.store(last_locked());
  } else if (dsync) {
    // No sync thread round: what isn't synced yet the caller syncs (sync()).
    if (durable_.load() < synced_.load())
      durable_.store(synced_.load());
  } else {
    {
      auto lk    = std::unique_lock{sync_mtx_};
      sync_wake_ = true;
    }
    sync_cv_.notify_all();
  }
  return ok();
}

Result<RaftIndex> Ledger::append(Entry const& e, std::span<uint8_t const> wal) {
  if (wal.size() != wal_size(e.cmd))
    return elog(system_error_code(EINVAL),
                "ledger {}: {} WAL bytes for {}",
                dir_,
                wal.size(),
                e.cmd);

  auto lk = std::unique_lock{mtx_};
  if (auto r = check_writable(); !r)
    return r.error();
  auto const last = last_locked();
  if (auto t = term_locked(last); t && e.term < *t)
    return elog(system_error_code(EINVAL), "ledger {}: term {} after term {}", dir_, e.term, *t);

  auto const idx = last + 1U;
  auto       buf = make_shared<string>();
  auto const crc = encode_record(
      *buf, idx, e, wal, files_.empty() ? base_crc_ : files_.rbegin()->second->last_crc);

  vector<Pending> p;
  p.push_back(Pending{
      .bytes = std::span<uint8_t const>(reinterpret_cast<uint8_t const*>(buf->data()), buf->size()),
      .entry = LedgerEntry{.idx    = idx,
                           .entry  = e,
                           .wal    = make_shared<Bytes const>(wal.begin(), wal.end()),
                           .crc    = crc,
                           .record = buf}});
  if (auto r = write_records(p); !r)
    return r.error();
  return idx;
}

Result<std::optional<RaftIndex>> Ledger::append_records(RaftIndex                prev_idx,
                                                        RaftTerm                 prev_term,
                                                        std::span<uint8_t const> records) {
  auto decoded = decode_records(records);
  if (!decoded)
    return elog(decoded.error(), "ledger {}: invalid records after index {}", dir_, prev_idx);
  auto r = append_records(prev_idx, prev_term, *decoded);
  if (!r)
    return r.error();
  if (!*r)
    return std::optional<RaftIndex>{};
  return std::optional<RaftIndex>{(*r)->last};
}

Result<std::optional<Appended>> Ledger::append_records(RaftIndex                    prev_idx,
                                                       RaftTerm                     prev_term,
                                                       vector<DecodedRecord> const& decoded,
                                                       bool                         sync) {
  if (!decoded.empty() && decoded.front().header.idx != prev_idx + 1U)
    return elog(system_error_code(EBADMSG),
                "ledger {}: records from index {} after index {}",
                dir_,
                decoded.front().header.idx,
                prev_idx);

  auto lk = std::unique_lock{mtx_};
  if (auto r = check_writable(); !r)
    return r.error();

  auto last = last_locked();
  if (prev_idx > last)
    return std::optional<Appended>{};

  // Records before the first entry are committed (compacted here): skip them.
  size_t k = 0;
  while (k < decoded.size() && decoded.at(k).header.idx <= base_idx_)
    ++k;
  auto const prev = k == 0 ? prev_idx : decoded.at(k - 1).header.idx;
  if (prev < base_idx_)
    return std::optional<Appended>{};  // nothing reaches this ledger's entries

  if (prev == prev_idx && prev > base_idx_)
    if (auto t = term_locked(prev); !t || *t != prev_term)
      return std::optional<Appended>{};

  auto const last_idx = decoded.empty() ? prev_idx : decoded.back().header.idx;
  if (k == decoded.size())
    return std::optional<Appended>{Appended{.last = last_idx, .first_new = last_idx + 1U}};

  // The chain: the first record follows the entry at prev.
  auto prev_crc = crc_locked(prev);
  if (!prev_crc)
    return prev_crc.error();
  if (*prev_crc && decoded.at(k).header.prev_crc != **prev_crc &&
      !(prev == base_idx_ && files_.empty()))
    return elog(system_error_code(EBADMSG),
                "ledger {}: the record at {} does not chain to the entry at {}",
                dir_,
                decoded.at(k).header.idx,
                prev);

  // Skip the records this ledger has; truncate at the first that differs.
  for (; k < decoded.size(); ++k) {
    auto const& rec = decoded.at(k);
    if (rec.header.idx > last)
      break;
    auto c = crc_locked(rec.header.idx);
    if (!c)
      return c.error();
    if (*c && **c == rec.header.crc)
      continue;
    if (auto r = truncate_locked(rec.header.idx - 1U); !r)
      return r.error();
    last = rec.header.idx - 1U;
    break;
  }

  auto const      first_new = k < decoded.size() ? decoded.at(k).header.idx : last_idx + 1U;
  vector<Pending> pending;
  for (; k < decoded.size(); ++k) {
    auto const& rec = decoded.at(k);
    pending.push_back(Pending{
        .bytes = rec.bytes,
        .entry = LedgerEntry{.idx   = rec.header.idx,
                             .entry = rec.entry,
                             .wal   = make_shared<Bytes const>(rec.wal.begin(), rec.wal.end()),
                             .crc   = rec.header.crc}});
  }
  if (!pending.empty())
    if (auto r = write_records(pending, sync); !r)
      return r.error();
  return std::optional<Appended>{Appended{.last = last_idx, .first_new = first_new}};
}

Result<void> Ledger::truncate_after(RaftIndex idx) {
  auto lk = std::unique_lock{mtx_};
  if (auto r = check_writable(); !r)
    return r;
  return truncate_locked(idx);
}

Result<void> Ledger::truncate_locked(RaftIndex idx) {
  auto const last = last_locked();
  if (idx >= last)
    return ok();
  if (idx < base_idx_)
    return elog(system_error_code(EINVAL),
                "ledger {}: truncating after {}, before the first index {}",
                dir_,
                idx,
                base_idx_ + 1U);

  auto c = crc_locked(idx);
  if (!c)
    return c.error();
  auto const crc = c->value_or(base_crc_);

  // Where the first dropped record is, if its file stays (it also holds idx).
  std::optional<std::pair<shared_ptr<File>, size_t>> at;
  if (auto it = files_.upper_bound(idx);
      it != files_.begin() && std::prev(it)->second->first <= idx &&
      idx < std::prev(it)->second->last) {
    auto loc = locate(idx + 1U);
    if (!loc)
      return loc.error();
    at = *loc;
  }

  ++generation_;
  while (!files_.empty() && files_.rbegin()->second->first > idx) {
    auto f = files_.rbegin()->second;
    if (::unlink(f->path.c_str()) != 0)
      return fail(elog(system_error_code(errno), "Unable to delete {}", f->path));
    files_.erase(std::prev(files_.end()));
  }

  if (at) {
    auto& [f, off] = *at;
    auto const to  = std::min(options_.file_size, f->end + kLedgerMarkerSize);
    f->written.store(u4(off), std::memory_order_release);
    if (auto r = zero_range(f->fd, off, to); !r)
      return fail(elog(r.error(), "Unable to truncate {}", f->path));
    if (!options_.no_sync && do_datasync(f->fd) != 0)
      return fail(elog(system_error_code(errno), "Unable to sync {}", f->path));
    f->end      = off;
    f->last     = idx;
    f->last_crc = crc;
    f->full     = false;
    f->publish();
    std::erase_if(f->sparse, [&](auto const& p) { return p.first > idx; });
  }
  if (auto r = fsync_dir(); !r)
    return fail(elog(r.error(), "Unable to sync {}", dir_));

  terms_.erase(terms_.upper_bound(idx), terms_.end());
  while (!window_.empty() && window_.back().idx > idx)
    window_.pop_back();
  if (durable_.load() > idx)
    durable_.store(idx);
  if (synced_.load() > idx)
    synced_.store(idx);

  // The state at idx: from the last file's header, with its records applied.
  if (files_.empty()) {
    tail_state_ = base_state_;
  } else {
    auto t = fold(*files_.rbegin()->second, files_.rbegin()->second->end);
    if (!t)
      return fail(t.error());
    tail_state_ = *t;
  }
  log(INFO, "ledger {}: truncated after index {}", dir_, idx);
  return ok();
}

Result<RaftState> Ledger::fold(File const& f, size_t limit) const {
  RaftState state = f.header.state;
  auto      res   = scan_file(f.fd,
                         options_.file_size,
                         limit,
                         f.start,
                         f.first,
                         /*verify=*/false,
                         f.header.prev_crc,
                         f.header.prev_term,
                         [&state](RecordHeader const& h, Entry const& e, size_t) {
                           state.apply(h.idx, e);
                         });
  if (!res)
    return res.error();
  return state;
}

Result<void> Ledger::sync_once() {
  shared_ptr<File> f;
  RaftIndex        last{0};
  uint64_t         gen = 0;
  {
    auto lk = std::shared_lock{mtx_};
    if (failed_)
      return failed_;
    if (!files_.empty())
      f = files_.rbegin()->second;
    last = last_locked();
    gen  = generation_;
  }
  if (f && !options_.no_sync && do_datasync(f->fd) != 0) {
    auto ec = system_error_code(errno);
    auto lk = std::unique_lock{mtx_};
    return fail(elog(ec, "Unable to sync {}", f->path));
  }
  auto lk = std::unique_lock{mtx_};
  if (gen == generation_ && durable_.load() < last)
    durable_.store(last);
  if (gen == generation_ && synced_.load() < last)
    synced_.store(last);
  return ok();
}

Result<void> Ledger::sync() {
  return sync_once();
}

void Ledger::set_durable_callback(std::function<void(RaftIndex)> cb) {
  auto lk     = std::unique_lock{sync_mtx_};
  durable_cb_ = std::move(cb);
}

void Ledger::sync_loop() {
  for (;;) {
    std::function<void(RaftIndex)> cb;
    {
      auto lk = std::unique_lock{sync_mtx_};
      if (options_.sync)
        sync_cv_.wait(lk, [this] { return sync_stop_ || sync_wake_; });
      else
        sync_cv_.wait_for(lk, std::chrono::seconds(1), [this] { return sync_stop_; });
      if (sync_stop_)
        return;
      sync_wake_ = false;
      if (options_.sync)
        cb = durable_cb_;
    }
    if (auto r = sync_once(); !r)
      continue;  // failed_ is set: every write fails from now on
    if (cb)
      cb(durable_.load());
  }
}

void Ledger::spare_loop() {
  auto const spare = dir_ / kSpareName;
  auto const tmp   = dir_ / kSpareTmpName;
  auto const stop  = [this] {
    auto lk = std::unique_lock{spare_mtx_};
    return spare_stop_;
  };

  for (;;) {
    {
      auto lk = std::unique_lock{spare_mtx_};
      spare_cv_.wait(lk, [this] { return spare_stop_ || !spare_ready_; });
      if (spare_stop_)
        return;
    }

    std::error_code ec;
    int             fd = ::open(tmp.c_str(), O_RDWR | O_CREAT | O_TRUNC, file_mode());
    if (fd == -1)
      ec = system_error_code(errno);
    static Bytes const zeros(kChunk, 0);
    for (size_t off = 0; !ec && off < options_.file_size; off += kChunk) {
      if (stop()) {
        ::close(fd);
        fs::remove(tmp, ec);
        return;
      }
      auto n = std::min(kChunk, options_.file_size - off);
      if (::pwrite(fd, zeros.data(), n, static_cast<off_t>(off)) != static_cast<ssize_t>(n))
        ec = system_error_code(errno ? errno : EIO);
    }
    if (!ec && !options_.no_sync && datasync(fd) != 0)
      ec = system_error_code(errno);
    if (fd != -1)
      ::close(fd);
    if (!ec && ::rename(tmp.c_str(), spare.c_str()) != 0)
      ec = system_error_code(errno);
    if (!ec && !options_.no_sync)
      if (auto r = fsync_path(dir_); !r)
        ec = r.error();

    auto lk = std::unique_lock{spare_mtx_};
    if (ec) {
      elog(ec, "ledger {}: unable to prepare {}", dir_, spare);
      spare_cv_.wait_for(lk, std::chrono::seconds(1), [this] { return spare_stop_; });
      continue;
    }
    spare_ready_ = true;
  }
}

// Reads.

RaftIndex Ledger::last_locked() const {
  return files_.empty() ? base_idx_ : files_.rbegin()->second->last;
}

tuple<RaftTerm, RaftIndex> Ledger::last_entry() const {
  auto       lk   = std::shared_lock{mtx_};
  auto const last = last_locked();
  return {term_locked(last).value_or(RaftTerm{0}), last};
}

RaftIndex Ledger::first_idx() const {
  auto lk = std::shared_lock{mtx_};
  return base_idx_ + 1U;
}

size_t Ledger::file_count() const {
  auto lk = std::shared_lock{mtx_};
  return files_.size();
}

RaftTerm Ledger::last_term_locked() const {
  return terms_.empty() ? base_term_ : terms_.rbegin()->second;
}

RaftState Ledger::base_state() const {
  auto lk = std::shared_lock{mtx_};
  return base_state_;
}

std::optional<RaftTerm> Ledger::term_locked(RaftIndex idx) const {
  if (idx == base_idx_)
    return base_term_;  // from the first file's header (0 for index 0)
  if (idx < base_idx_ || idx > last_locked())
    return std::nullopt;
  auto it = terms_.upper_bound(idx);
  if (it == terms_.begin())
    return std::nullopt;
  return std::prev(it)->second;
}

std::optional<RaftTerm> Ledger::term(RaftIndex idx) const {
  auto lk = std::shared_lock{mtx_};
  return term_locked(idx);
}

std::optional<Ledger::Cursor> Ledger::cursor_locked(RaftIndex idx) const {
  auto it = files_.upper_bound(idx);
  if (it == files_.begin())
    return std::nullopt;
  auto const& f = std::prev(it)->second;
  if (idx < f->first || idx > f->last)
    return std::nullopt;
  auto sp = std::ranges::upper_bound(
      f->sparse, idx, std::less<>{}, [](auto const& p) { return p.first; });
  assert(sp != f->sparse.begin());
  --sp;
  return Cursor{.f     = f,
                .at    = sp->first,
                .off   = sp->second,
                .last  = f->last,
                .limit = f->written.load(std::memory_order_acquire)};
}

std::optional<Ledger::Cursor> Ledger::next_cursor_locked(Cursor const& c) const {
  auto it = files_.upper_bound(c.f->first);
  if (it == files_.end())
    return std::nullopt;
  auto const& f = it->second;
  return Cursor{.f     = f,
                .at    = f->first,
                .off   = f->start,
                .last  = f->last,
                .limit = f->written.load(std::memory_order_acquire)};
}

Result<void> Ledger::seek(Cursor& c, RaftIndex idx) const {
  LedgerReader r(c.f->fd, options_.file_size, c.limit);
  return seek(c, r, idx);
}

Result<void> Ledger::seek(Cursor& c, LedgerReader& r, RaftIndex idx) const {
  while (c.at < idx) {
    auto n = next_slot(r, c.off, options_.file_size, /*verify=*/false);
    if (!n)
      return n.error();
    if (n->slot != Slot::kRecord || n->h.idx != c.at)
      return elog(system_error_code(EBADMSG),
                  "ledger {}: no record {} at offset {} of {}",
                  dir_,
                  c.at,
                  c.off,
                  c.f->path);
    c.off += n->size;
    ++c.at;
  }
  return ok();
}

Result<std::optional<std::pair<shared_ptr<Ledger::File>, size_t>>> Ledger::locate(
    RaftIndex idx) const {
  auto c = cursor_locked(idx);
  if (!c)
    return std::nullopt;
  if (auto r = seek(*c, idx); !r)
    return r.error();
  return std::pair{c->f, c->off};
}

Result<uint32_t> Ledger::crc_at(Cursor c, RaftIndex idx) const {
  LedgerReader r(c.f->fd, options_.file_size, c.limit);
  if (auto s = seek(c, r, idx); !s)
    return s.error();
  auto n = next_slot(r, c.off, options_.file_size, /*verify=*/false);
  if (!n)
    return n.error();
  if (n->slot != Slot::kRecord || n->h.idx != idx)
    return system_error_code(EBADMSG);
  return n->h.crc;
}

/// The cases answered from memory; nullopt when the record must be read.
std::optional<std::optional<uint32_t>> Ledger::crc_in_memory(RaftIndex idx) const {
  if (idx == base_idx_)
    return std::optional<uint32_t>{base_crc_};
  if (idx < base_idx_ || idx > last_locked())
    return std::optional<uint32_t>{};
  if (idx == last_locked())
    return std::optional<uint32_t>{files_.rbegin()->second->last_crc};
  if (!window_.empty() && idx >= window_.front().idx)
    return std::optional<uint32_t>{window_.at(INT(idx) - INT(window_.front().idx)).crc};
  return std::nullopt;
}

Result<std::optional<uint32_t>> Ledger::crc_locked(RaftIndex idx) const {
  if (auto m = crc_in_memory(idx); m)
    return *m;
  auto c = cursor_locked(idx);
  if (!c)
    return std::optional<uint32_t>{};
  auto crc = crc_at(*c, idx);
  if (!crc)
    return crc.error();
  return std::optional<uint32_t>{*crc};
}

Result<std::optional<uint32_t>> Ledger::crc(RaftIndex idx) const {
  std::optional<Cursor> c;
  {
    auto lk = std::shared_lock{mtx_};
    if (auto m = crc_in_memory(idx); m)
      return *m;
    c = cursor_locked(idx);
  }
  // The reads, without the lock: before the written offset.
  if (!c)
    return std::optional<uint32_t>{};
  auto crc = crc_at(*c, idx);
  if (!crc)
    return crc.error();
  return std::optional<uint32_t>{*crc};
}

Result<std::optional<RecordBatch>> Ledger::records(RaftIndex idx,
                                                   size_t    count,
                                                   size_t    max_bytes) const {
  // Where to read, under the lock; the reads, without it (each file before its written offset).
  inplace_vector<Cursor, 2> cursors;
  RaftIndex                 last{0};
  vector<shared_ptr<string const>> recent;  // the records from the window, when it has them
  {
    auto lk = std::shared_lock{mtx_};
    if (idx <= base_idx_)
      return std::optional<RecordBatch>{};
    last = last_locked();

    // The leader's latest records are in the window with their bytes (append()): no seek, no read.
    if (idx <= last && count > 0 && !window_.empty() && idx >= window_.front().idx) {
      size_t bytes = 0;
      for (auto i = INT(idx) - INT(window_.front().idx); i < window_.size() && recent.size() < count;
           ++i) {
        auto const& e = window_.at(i);
        if (!e.record || e.idx != idx + recent.size())
          break;
        if (bytes + e.record->size() > max_bytes && !recent.empty())
          break;
        bytes += e.record->size();
        recent.push_back(e.record);
      }
    }
    if (recent.empty() && idx <= last && count > 0) {
      auto c = cursor_locked(idx);
      if (!c)
        return elog(system_error_code(EBADMSG), "ledger {}: no file holds index {}", dir_, idx);
      cursors.push_back(*c);
      if (auto n = next_cursor_locked(*c); n)
        cursors.push_back(*n);
    }
  }

  RecordBatch batch{.first = idx, .last = idx - 1U};
  if (!recent.empty()) {
    size_t total = 0;
    for (auto const& r : recent)
      total += r->size();
    Bytes buf;
    buf.reserve(total);
    for (auto const& r : recent)
      buf.insert(buf.end(), r->begin(), r->end());
    batch.last = idx + recent.size() - 1U;
    batch.buffers.push_back(std::move(buf));
    return std::optional<RecordBatch>{std::move(batch)};
  }
  size_t bytes = 0;
  auto   cur   = idx;
  for (auto& c : cursors) {
    LedgerReader r(c.f->fd, options_.file_size, c.limit);
    if (auto s = seek(c, r, cur); !s)
      return s.error();
    size_t const start = c.off;
    while (cur <= c.last && cur <= last && batch.count() < count) {
      auto n = next_slot(r, c.off, options_.file_size, /*verify=*/false);
      if (!n)
        return n.error();
      if (n->slot != Slot::kRecord || n->h.idx != cur)
        return elog(system_error_code(EBADMSG),
                    "ledger {}: no record {} at offset {} of {}",
                    dir_,
                    cur,
                    c.off,
                    c.f->path);
      if (bytes + n->size > max_bytes && batch.last >= batch.first)
        break;
      bytes += n->size;
      c.off += n->size;
      batch.last = cur;
      ++cur;
    }
    if (c.off > start) {
      Bytes  buf(c.off - start);
      size_t got = 0;
      while (got < buf.size()) {
        auto n =
            ::pread(c.f->fd, buf.data() + got, buf.size() - got, static_cast<off_t>(start + got));
        if (n < 0 && errno == EINTR)
          continue;
        if (n <= 0)
          return elog(system_error_code(n < 0 ? errno : EIO), "Unable to read {}", c.f->path);
        got += static_cast<size_t>(n);
      }
      batch.buffers.push_back(std::move(buf));
    }
    if (cur > last || batch.count() >= count || cur <= c.last)
      break;  // the end, the count, or the byte limit
  }
  return std::optional<RecordBatch>{std::move(batch)};
}

Result<std::optional<vector<LedgerEntry>>> Ledger::entries(RaftIndex idx,
                                                           size_t    count,
                                                           size_t    max_bytes) const {
  vector<LedgerEntry> out;
  {
    auto lk = std::shared_lock{mtx_};
    if (idx <= base_idx_)
      return std::optional<vector<LedgerEntry>>{};

    if (!window_.empty() && idx >= window_.front().idx) {
      size_t bytes = 0;
      for (auto i = INT(idx) - INT(window_.front().idx); i < window_.size() && out.size() < count;
           ++i) {
        auto const& e = window_.at(i);
        bytes += e.wal->size();
        if (bytes > max_bytes && !out.empty())
          break;
        out.push_back(e);
      }
      return std::optional<vector<LedgerEntry>>{std::move(out)};
    }
  }

  // From the files. A batch of records spans at most two files: as many batches as it takes.
  size_t bytes = 0;
  for (auto next = idx; out.size() < count;) {
    auto batch = records(next, count - out.size(), max_bytes - std::min(bytes, max_bytes));
    if (!batch)
      return batch.error();
    if (!*batch)
      return std::optional<vector<LedgerEntry>>{};  // compacted meanwhile
    if ((*batch)->count() == 0)
      break;
    for (auto const& buf : (*batch)->buffers) {
      bytes += buf.size();
      auto decoded = decode_records(buf);
      if (!decoded)
        return elog(decoded.error(), "ledger {}: invalid records from index {}", dir_, next);
      for (auto& rec : *decoded)
        out.push_back(LedgerEntry{.idx   = rec.header.idx,
                                  .entry = std::move(rec.entry),
                                  .wal   = make_shared<Bytes const>(rec.wal.begin(), rec.wal.end()),
                                  .crc   = rec.header.crc});
    }
    next = (*batch)->last + 1U;
    if (bytes >= max_bytes)
      break;
  }
  return std::optional<vector<LedgerEntry>>{std::move(out)};
}

Result<void> Ledger::verify() const {
  struct Snapshot {
    shared_ptr<File> f;
    RaftIndex        last;
    size_t           end;
    size_t           limit;  // the written offset; the whole file once it is full (its marker)
  };
  vector<Snapshot> files;
  {
    auto lk = std::shared_lock{mtx_};
    for (auto const& [_, f] : files_)
      files.push_back(Snapshot{.f     = f,
                               .last  = f->last,
                               .end   = f->end,
                               .limit = f->full ? options_.file_size
                                                : f->written.load(std::memory_order_acquire)});
  }

  for (size_t i = 0; i < files.size(); ++i) {
    auto const& [f, last, end, limit] = files.at(i);
    bool const is_last                = i + 1 == files.size();
    auto       h                      = read_file_header(f->fd, options_.file_size, limit);
    if (!h || !(h->first == f->header))
      return elog(system_error_code(EBADMSG), "ledger {}: {} has no valid header", dir_, f->path);
    if (i > 0) {
      auto const& prev = *files.at(i - 1).f;
      if (h->first.first != files.at(i - 1).last + 1U || h->first.prev_crc != prev.last_crc)
        return elog(system_error_code(EBADMSG),
                    "ledger {}: {} does not continue the previous file",
                    dir_,
                    f->path);
    }
    auto res = scan_file(f->fd,
                         options_.file_size,
                         limit,
                         h->second,
                         f->first,
                         /*verify=*/true,
                         h->first.prev_crc,
                         h->first.prev_term,
                         nullptr);
    if (!res)
      return elog(res.error(), "Unable to read {}", f->path);
    bool const complete = res->stop == Stop::kMarker || res->stop == Stop::kFull;
    if (res->stop == Stop::kBad || res->last != last || res->end != end ||
        (!is_last && !complete))
      return elog(system_error_code(EBADMSG),
                  "ledger {}: {} does not verify: it ends at index {} (expected {}), offset {}",
                  dir_,
                  f->path,
                  res->last,
                  last,
                  res->end);
  }
  return ok();
}

void Ledger::release(RaftIndex idx) {
  auto lk = std::unique_lock{mtx_};
  if (idx > released_)
    released_ = idx;
  while (!window_.empty() && window_.front().idx <= idx)
    window_.pop_front();
}

// Compaction.

RaftIndex Ledger::compaction_point(RaftIndex idx) const {
  auto lk = std::shared_lock{mtx_};
  if (files_.empty())
    return base_idx_ + 1U;
  auto it = files_.upper_bound(idx);
  if (it == files_.begin())
    return it->first;
  return std::prev(it)->first;
}

RaftIndex Ledger::compaction_point_before_wal(uint64_t lsn, RaftIndex limit) const {
  auto lk = std::shared_lock{mtx_};
  if (files_.empty())
    return base_idx_ + 1U;
  auto best = files_.begin()->first;
  for (auto const& [first, f] : files_) {
    if (first > limit || f->header.state.wal_end.lsn > lsn)
      break;
    best = first;
  }
  return best;
}

Result<void> Ledger::drop_before(RaftIndex first) {
  auto paths = detach_before(first);
  if (!paths)
    return paths.error();
  return unlink_files(*paths);
}

Result<void> Ledger::unlink_files(vector<fs::path> const& paths) {
  if (paths.empty())
    return ok();
  for (auto const& path : paths)
    if (::unlink(path.c_str()) != 0 && errno != ENOENT)
      return elog(system_error_code(errno), "Unable to delete {}", path);
  return fsync_dir();
}

Result<vector<fs::path>> Ledger::detach_before(RaftIndex first) {
  vector<fs::path> paths;
  auto lk = std::unique_lock{mtx_};
  auto it = files_.find(first);
  if (it == files_.end())
    return elog(system_error_code(EINVAL), "ledger {}: no file starts at index {}", dir_, first);
  if (it == files_.begin())
    return paths;

  auto const& h = it->second->header;
  base_idx_     = first - 1U;
  base_crc_     = h.prev_crc;
  base_term_    = h.prev_term;
  base_state_   = h.state;
  while (files_.begin()->first < first) {
    paths.push_back(files_.begin()->second->path);
    files_.erase(files_.begin());
  }
  auto const term_at = terms_.upper_bound(first) == terms_.begin()
                           ? std::optional<RaftTerm>{}
                           : std::optional<RaftTerm>{std::prev(terms_.upper_bound(first))->second};
  terms_.erase(terms_.begin(), terms_.lower_bound(first));
  if (term_at && (terms_.empty() || terms_.begin()->first != first))
    terms_[first] = *term_at;
  while (!window_.empty() && window_.front().idx < first)
    window_.pop_front();
  return paths;
}

// InstallSnapshot.

std::optional<SnapshotFile> Ledger::snapshot_file() const {
  auto lk = std::shared_lock{mtx_};
  if (files_.empty() || last_locked() < files_.begin()->first)
    return std::nullopt;
  auto const& f = files_.begin()->second;
  return SnapshotFile{.first = f->first, .size = f->written.load(std::memory_order_acquire)};
}

Result<std::optional<Bytes>> Ledger::read_snapshot(RaftIndex first, size_t offset, size_t size) const {
  shared_ptr<File> f;
  {
    auto lk = std::shared_lock{mtx_};
    if (auto it = files_.find(first); it != files_.end())
      f = it->second;
  }
  if (!f)
    return std::optional<Bytes>{};
  // Without the lock: before the written offset.
  if (offset + size > f->written.load(std::memory_order_acquire))
    return elog(system_error_code(EINVAL),
                "ledger {}: reading {} bytes at {} of {}, past its written offset",
                dir_,
                size,
                offset,
                f->path);
  Bytes  buf(size);
  size_t got = 0;
  while (got < size) {
    auto n = ::pread(f->fd, buf.data() + got, size - got, static_cast<off_t>(offset + got));
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return elog(system_error_code(n < 0 ? errno : EIO), "Unable to read {}", f->path);
    got += static_cast<size_t>(n);
  }
  return std::optional<Bytes>{std::move(buf)};
}

Result<void> Ledger::install_chunk(size_t offset, std::span<uint8_t const> bytes) {
  auto lk   = std::unique_lock{mtx_};
  auto path = dir_ / kInstallName;
  if (offset != 0 && offset != install_size_)
    return elog(system_error_code(EINVAL),
                "ledger {}: an install chunk at {}, after {} bytes",
                dir_,
                offset,
                install_size_);
  if (offset + bytes.size() > options_.file_size)
    return elog(system_error_code(EFBIG), "ledger {}: {} bytes to install", dir_, offset + bytes.size());

  int fd = offset == 0 ? ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, file_mode())
                       : ::open(path.c_str(), O_RDWR);
  if (fd == -1)
    return elog(system_error_code(errno), "Unable to open {}", path);
  Result<void> r = ok();
  if (offset == 0 && ::ftruncate(fd, static_cast<off_t>(options_.file_size)) != 0)
    r = system_error_code(errno);
  if (r)
    r = pwrite_all(fd, bytes.data(), bytes.size(), static_cast<off_t>(offset));
  ::close(fd);
  if (!r) {
    install_size_ = 0;
    return elog(r.error(), "Unable to write {}", path);
  }
  install_size_ = offset + bytes.size();
  return ok();
}

Result<InstallInfo> Ledger::prepare_install() {
  auto lk   = std::unique_lock{mtx_};
  auto path = dir_ / kInstallName;
  int  fd   = ::open(path.c_str(), O_RDWR);
  if (fd == -1)
    return elog(system_error_code(errno), "Unable to open {}", path);
  auto const size  = install_size_;
  auto       close = [fd](Result<InstallInfo> r) {
    ::close(fd);
    return r;
  };

  auto h = read_file_header(fd, options_.file_size, size);
  if (!h)
    return close(elog(h.error(), "ledger {}: the install file has no valid header", dir_));

  InstallInfo info{.header = h->first, .last = h->first.first - 1U, .first_segment = {}};
  auto        res = scan_file(fd,
                       options_.file_size,
                       size,
                       h->second,
                       h->first.first,
                       /*verify=*/true,
                       h->first.prev_crc,
                       h->first.prev_term,
                       [&info](RecordHeader const&, Entry const& e, size_t) {
                         if (!info.first_segment && e.cmd.segment)
                           info.first_segment = e.cmd.segment;
                       });
  if (!res)
    return close(elog(res.error(), "Unable to read {}", path));
  // Every byte sent is a record: the scan ends where the bytes do.
  if (res->stop != Stop::kEnd || res->end != size || res->last < h->first.first)
    return close(elog(system_error_code(EBADMSG),
                      "ledger {}: the install file ends at index {}, offset {} of {}",
                      dir_,
                      res->last,
                      res->end,
                      size));
  info.last = res->last;
  if (!options_.no_sync && do_datasync(fd) != 0)
    return close(elog(system_error_code(errno), "Unable to sync {}", path));
  if (auto r = fsync_dir(); !r)
    return close(r.error());
  return close(info);
}

Result<void> Ledger::finish_install(RaftIndex first) {
  auto lk      = std::unique_lock{mtx_};
  auto install = dir_ / kInstallName;
  if (!fs::exists(install))
    return elog(system_error_code(ENOENT), "ledger {}: no install file", dir_);

  files_.clear();  // closes them
  for (auto const& entry : fs::directory_iterator(dir_)) {
    if (parse_file_name(entry.path().filename().string()) && ::unlink(entry.path().c_str()) != 0)
      return fail(elog(system_error_code(errno), "Unable to delete {}", entry.path()));
  }
  auto to = dir_ / file_name(first);
  if (do_rename(install.c_str(), to.c_str()) != 0)
    return fail(elog(system_error_code(errno), "Unable to rename {} to {}", install, to));
  if (auto r = fsync_dir(); !r)
    return fail(elog(r.error(), "Unable to sync {}", dir_));

  ++generation_;
  if (auto r = load_files(first); !r)
    return fail(r.error());
  released_ = last_locked();
  durable_.store(released_);
  synced_.store(released_);
  log(INFO, "ledger {}: installed {} … {}", dir_, base_idx_ + 1U, released_);
  return ok();
}

// The term and meta files.

Result<void> write_checked_file(fs::path const& path, string const& contents, bool no_sync) {
  string out(kCheckedHeader, '\0');
  auto*  p = reinterpret_cast<uint8_t*>(out.data());
  put_le<uint32_t>(p, kCheckedMagic);
  put_le<uint32_t>(p + 4, crc32c(0, contents.data(), contents.size()));
  put_le<uint64_t>(p + 8, contents.size());
  out += contents;

  auto tmp = path;
  tmp += ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, file_mode());
  if (fd == -1)
    return elog(system_error_code(errno), "Unable to create {}", tmp);
  auto n   = ::write(fd, out.data(), out.size());
  auto err = errno;
  if (n == static_cast<ssize_t>(out.size()) && !no_sync && ::fsync(fd) != 0) {
    err = errno;
    n   = -1;
  }
  ::close(fd);
  if (n != static_cast<ssize_t>(out.size()))
    return elog(system_error_code(n < 0 ? err : EIO), "Unable to write {}", tmp);
  if (::rename(tmp.c_str(), path.c_str()) != 0)
    return elog(system_error_code(errno), "Unable to rename {} to {}", tmp, path);
  if (!no_sync)
    if (auto r = fsync_path(path.parent_path()); !r)
      return elog(r.error(), "Unable to sync {}", path.parent_path());
  return ok();
}

Result<string> read_checked_file(fs::path const& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in)
    return system_error_code(errno == 0 ? ENOENT : errno);
  string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  if (data.size() < kCheckedHeader)
    return elog(system_error_code(EBADMSG), "{}: {} bytes", path, data.size());
  auto const* p = reinterpret_cast<uint8_t const*>(data.data());
  if (get_le<uint32_t>(p) != kCheckedMagic || get_le<uint64_t>(p + 8) != data.size() - kCheckedHeader)
    return elog(system_error_code(EBADMSG), "{}: not a checked file", path);
  string contents = data.substr(kCheckedHeader);
  if (crc32c(0, contents.data(), contents.size()) != get_le<uint32_t>(p + 4))
    return elog(system_error_code(EBADMSG), "{}: checksum mismatch", path);
  return contents;
}

namespace {

template <typename T>
string serialize_to_string(T const& t) {
  string      out;
  RawOArchive oa(out);
  oa << t;
  return out;
}

template <typename T>
Result<T> read_object(fs::path const& path) {
  std::error_code ec;
  if (!fs::exists(path, ec))
    return T{};
  auto contents = read_checked_file(path);
  if (!contents)
    return contents.error();
  try {
    T           t{};
    RawIArchive ia(contents->data(), contents->size());
    ia >> t;
    return t;
  } catch (std::exception const&) {
    return elog(system_error_code(EBADMSG), "{}: unable to decode", path);
  }
}

}  // namespace

Result<TermVote> TermFile::open() {
  auto t = read_term_file(path_);
  if (!t)
    return t.error();
  auto lk = std::unique_lock{mtx_};
  tv_     = *t;
  return tv_;
}

TermVote TermFile::get() const {
  auto lk = std::unique_lock{mtx_};
  return tv_;
}

Result<RaftTerm> TermFile::set_current_term(RaftTerm term) {
  auto lk = std::unique_lock{mtx_};
  if (tv_.term < term) {
    auto next = TermVote{.term = term, .voted_for = std::nullopt};
    if (auto r = write_term_file(path_, next, no_sync_); !r)
      return r.error();
    tv_ = next;
  }
  return tv_.term;
}

Result<bool> TermFile::vote(RaftTerm term, RaftNodeId node) {
  auto lk = std::unique_lock{mtx_};
  if (tv_.term != term) {
    log(ERROR, "The term does not match {} vs {}", term, tv_.term);
    return false;
  }
  if (tv_.voted_for)
    return *tv_.voted_for == node;
  auto next = TermVote{.term = term, .voted_for = node};
  if (auto r = write_term_file(path_, next, no_sync_); !r)
    return r.error();
  tv_ = next;
  return true;
}

Result<void> write_term_file(fs::path const& path, TermVote const& t, bool no_sync) {
  return write_checked_file(path, serialize_to_string(t), no_sync);
}

Result<TermVote> read_term_file(fs::path const& path) {
  return read_object<TermVote>(path);
}

Result<void> write_meta_file(fs::path const& path, LedgerMeta const& m, bool no_sync) {
  return write_checked_file(path, serialize_to_string(m), no_sync);
}

Result<LedgerMeta> read_meta_file(fs::path const& path) {
  return read_object<LedgerMeta>(path);
}
