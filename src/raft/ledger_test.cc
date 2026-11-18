#define BOOST_TEST_MODULE ledger
#include "src/raft/ledger.h"

#include <random>

#include "src/base/testing.h"

// The ledger (docs/ledger.md): the record format, appends across files (with and without end
// markers), reopening, the follower's appends of the leader's records, truncation, torn tails and
// stale records, compaction, InstallSnapshot, the sync and spare threads, write failures, and the
// term and meta files.

namespace fs = std::filesystem;

namespace {

LedgerOptions small(size_t file_size = 4096) {
  return LedgerOptions{
      .file_size = file_size, .sparse_step = 256, .sync = false, .no_sync = true, .spare = false};
}

Bytes random_bytes(size_t n, uint32_t seed) {
  std::mt19937 gen(seed);
  Bytes        b(n);
  for (auto& c : b)
    c = static_cast<uint8_t>(gen());
  return b;
}

struct Item {
  Entry e;
  Bytes wal;
};

Item flush(RaftTerm term, size_t n, uint32_t seed) {
  return Item{
      .e   = Entry{.time = std::time_t{1700000000} + seed,
                   .term = term,
                   .cmd  = WalCommand{.flushed_lsn = LogSeqNumber{1, seed},
                                      .segment     = LogSeqNumber{1, 0x1000000},
                                      .command     = FlushOp{.offset = static_cast<off_t>(seed % 4096),
                                                             .size   = n}}},
      .wal = random_bytes(n, seed),
  };
}

Item tick(RaftTerm term, std::time_t time = 1700000000) {
  return Item{.e = Entry{.time = time, .term = term, .cmd = WalCommand{}}, .wal = {}};
}

Item timeline(RaftTerm term) {
  return Item{.e   = Entry{.time = 1700000000,
                           .term = term,
                           .cmd  = WalCommand{.command = TimelineOp{
                                                  .timeline = 2,
                                                  .content  = "1\t0/3000000\tno recovery target\n"}}},
              .wal = {}};
}

/// Appends |n| flushes of up to |max_wal| bytes with |term|; returns them.
vector<Item> append_n(Ledger& l, size_t n, RaftTerm term, uint32_t seed, size_t max_wal = 600) {
  vector<Item> items;
  for (size_t i = 0; i < n; ++i) {
    auto s    = seed + static_cast<uint32_t>(i);
    auto item = flush(term, s % (max_wal + 1), s);
    auto idx  = l.append(item.e, item.wal);
    BOOST_REQUIRE(idx.has_value());
    items.push_back(std::move(item));
  }
  return items;
}

void check_entries(Ledger const& l, RaftIndex from, vector<Item> const& items) {
  auto got = l.entries(from, items.size(), size_t{1} << 30U);
  BOOST_REQUIRE(got.has_value());
  BOOST_REQUIRE(got->has_value());
  BOOST_REQUIRE_EQUAL((*got)->size(), items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    auto const& e = (*got)->at(i);
    BOOST_TEST(e.idx == from + i);
    BOOST_TEST((e.entry == items.at(i).e));
    BOOST_TEST((*e.wal == items.at(i).wal));
  }
}

/// The concatenation of a batch's buffers.
Bytes concat(RecordBatch const& b) {
  Bytes out;
  for (auto const& buf : b.buffers)
    out.insert(out.end(), buf.begin(), buf.end());
  return out;
}

/// An InstallSnapshot: |from|'s first file, in chunks of |chunk| bytes, replaces |to|'s ledger.
InstallInfo install(Ledger const& from, Ledger& to, size_t chunk = 1000) {
  auto file = from.snapshot_file();
  BOOST_REQUIRE(file.has_value());
  for (size_t off = 0; off < file->size; off += chunk) {
    auto n     = std::min(chunk, file->size - off);
    auto bytes = from.read_snapshot(file->first, off, n);
    BOOST_REQUIRE(bytes.has_value() && bytes->has_value());
    BOOST_REQUIRE(to.install_chunk(off, **bytes).has_value());
  }
  auto info = to.prepare_install();
  BOOST_REQUIRE(info.has_value());
  BOOST_TEST(info->header.first == file->first);
  BOOST_REQUIRE(to.finish_install(file->first).has_value());
  return *info;
}

/// The state of the entries before |n| (items[0, n)), as the file headers hold it.
RaftState state_after(vector<Item> const& items, size_t n) {
  RaftState s;
  for (size_t i = 0; i < n; ++i)
    s.apply(RaftIndex{i + 1}, items.at(i).e);
  return s;
}

/// Copies every record of |from| after the entry at |prev| into |to| (batches of 7).
void replicate(Ledger const& from, Ledger& to, RaftIndex prev) {
  for (;;) {
    auto batch = from.records(prev + 1U, 7, 2000);
    BOOST_REQUIRE(batch.has_value() && batch->has_value());
    if ((*batch)->count() == 0)
      return;
    BOOST_TEST((*batch)->buffers.size() <= 2U);
    auto bytes = concat(**batch);
    auto r     = to.append_records(prev, *from.term(prev), bytes);
    BOOST_REQUIRE(r.has_value());
    BOOST_REQUIRE(r->has_value());
    BOOST_TEST(**r == (*batch)->last);
    prev = (*batch)->last;
  }
}

/// The ledger files of |dir|, sorted.
vector<fs::path> ledger_files(fs::path const& dir) {
  vector<fs::path> out;
  for (auto const& e : fs::directory_iterator(dir))
    if (e.path().extension() == ".ledger" && e.path().filename().string().front() != '.')
      out.push_back(e.path());
  std::ranges::sort(out);
  return out;
}

Bytes read_file(fs::path const& path) {
  std::ifstream in(path, std::ios::binary);
  return Bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

void write_at(fs::path const& path, size_t off, std::span<uint8_t const> bytes) {
  int fd = ::open(path.c_str(), O_RDWR);
  BOOST_REQUIRE(fd != -1);
  BOOST_REQUIRE(::pwrite(fd, bytes.data(), bytes.size(), static_cast<off_t>(off)) ==
                static_cast<ssize_t>(bytes.size()));
  ::close(fd);
}

/// The size of a file header (fixed: its fields are).
size_t file_header_size() {
  return encode_file_header(FileHeader{.first = RaftIndex{1}}).size();
}

/// The offsets of the records of a ledger file, and the end.
std::pair<vector<size_t>, size_t> record_offsets(fs::path const& path) {
  auto           data = read_file(path);
  vector<size_t> offs;
  auto           h    = decode_file_header(data);
  BOOST_REQUIRE(h.has_value());
  size_t off = h->second;
  while (off + kLedgerHeaderSize <= data.size()) {
    auto r = decode_record(std::span<uint8_t const>(data).subspan(off));
    if (!r)
      break;
    offs.push_back(off);
    off += r->bytes.size();
  }
  return {offs, off};
}

/// A directory of its own under the test case's TmpDir (which is one directory per test case),
/// emptied first.
struct Dir {
  Dir(TmpDir const& base, string const& name) : path_(base() / name) {
    fs::remove_all(path_);
    fs::create_directories(path_);
  }
  fs::path const& operator()() const { return path_; }

  fs::path path_;
};

/// items[from, to)
vector<Item> slice(vector<Item> const& v, uint64_t from, uint64_t to = UINT64_MAX) {
  to = std::min<uint64_t>(to, v.size());
  return vector<Item>(v.begin() + static_cast<std::ptrdiff_t>(from),
                      v.begin() + static_cast<std::ptrdiff_t>(to));
}

size_t tick_size() {
  string out;
  auto   t = tick(RaftTerm{1});
  (void)encode_record(out, RaftIndex{1}, t.e, {}, 0);
  return out.size();
}

}  // namespace

BOOST_AUTO_TEST_CASE(record_round_trip) {
  TmpDir base;
  vector<Item> items{flush(RaftTerm{3}, 100, 1), tick(RaftTerm{3}), timeline(RaftTerm{4}),
                     flush(RaftTerm{4}, 0, 2)};
  string       all;
  uint32_t     prev = 0x1234;
  for (size_t i = 0; i < items.size(); ++i) {
    string one;
    auto   crc = encode_record(one, RaftIndex{10 + i}, items.at(i).e, items.at(i).wal, prev);
    auto   r   = decode_record(std::span<uint8_t const>(
        reinterpret_cast<uint8_t const*>(one.data()), one.size()));
    BOOST_REQUIRE(r.has_value());
    BOOST_TEST(r->header.idx == RaftIndex{10 + i});
    BOOST_TEST(r->header.term == items.at(i).e.term);
    BOOST_TEST(r->header.prev_crc == prev);
    BOOST_TEST(r->header.crc == crc);
    BOOST_TEST((r->entry == items.at(i).e));
    BOOST_TEST((Bytes(r->wal.begin(), r->wal.end()) == items.at(i).wal));
    BOOST_TEST(r->bytes.size() == one.size());

    // Any byte changed, or the record cut short: rejected.
    for (size_t b = 0; b < one.size(); ++b) {
      auto bad = one;
      bad.at(b) = static_cast<char>(bad.at(b) ^ 0x10);
      BOOST_TEST(!decode_record(std::span<uint8_t const>(
                                    reinterpret_cast<uint8_t const*>(bad.data()), bad.size()))
                      .has_value());
    }
    BOOST_TEST(!decode_record(std::span<uint8_t const>(
                                  reinterpret_cast<uint8_t const*>(one.data()), one.size() - 1))
                    .has_value());
    all += one;
    prev = crc;
  }

  auto span = std::span<uint8_t const>(reinterpret_cast<uint8_t const*>(all.data()), all.size());
  auto recs = decode_records(span);
  BOOST_REQUIRE(recs.has_value());
  BOOST_TEST(recs->size() == items.size());

  // A broken chain, a gap, a decreasing term.
  string broken;
  auto   c1 = encode_record(broken, RaftIndex{1}, items.at(0).e, items.at(0).wal, 0);
  (void)encode_record(broken, RaftIndex{2}, items.at(1).e, items.at(1).wal, c1 + 1);
  BOOST_TEST(!decode_records(std::span<uint8_t const>(
                                 reinterpret_cast<uint8_t const*>(broken.data()), broken.size()))
                  .has_value());
  string gap;
  c1 = encode_record(gap, RaftIndex{1}, items.at(0).e, items.at(0).wal, 0);
  (void)encode_record(gap, RaftIndex{3}, items.at(1).e, items.at(1).wal, c1);
  BOOST_TEST(!decode_records(
                  std::span<uint8_t const>(reinterpret_cast<uint8_t const*>(gap.data()), gap.size()))
                  .has_value());
  string down;
  auto   later = tick(RaftTerm{5});
  c1           = encode_record(down, RaftIndex{1}, later.e, {}, 0);
  (void)encode_record(down, RaftIndex{2}, items.at(0).e, items.at(0).wal, c1);
  BOOST_TEST(!decode_records(std::span<uint8_t const>(
                                 reinterpret_cast<uint8_t const*>(down.data()), down.size()))
                  .has_value());
}

BOOST_AUTO_TEST_CASE(append_and_reopen) {
  TmpDir base;
  Dir tmp(base, "tmp1");
  vector<Item> items;
  {
    Ledger l(tmp(), small());
    BOOST_REQUIRE(l.open().has_value());
    BOOST_TEST((l.last_entry() == tuple<RaftTerm, RaftIndex>{RaftTerm{0}, RaftIndex{0}}));
    BOOST_TEST(l.first_idx() == RaftIndex{1});
    items        = append_n(l, 100, RaftTerm{1}, 1);
    auto more    = append_n(l, 100, RaftTerm{2}, 1000);
    items.insert(items.end(), more.begin(), more.end());
    BOOST_TEST(l.file_count() > 5U);
    BOOST_TEST((l.last_entry() == tuple<RaftTerm, RaftIndex>{RaftTerm{2}, RaftIndex{200}}));
    BOOST_TEST(*l.term(RaftIndex{100}) == RaftTerm{1});
    BOOST_TEST(*l.term(RaftIndex{101}) == RaftTerm{2});
    BOOST_TEST(!l.term(RaftIndex{201}).has_value());
    BOOST_TEST(l.durable_idx() == RaftIndex{200});
    check_entries(l, RaftIndex{1}, items);
    BOOST_TEST(l.verify().has_value());
  }
  // Every file is exactly the file size.
  for (auto const& p : ledger_files(tmp()))
    BOOST_TEST(fs::file_size(p) == 4096U);

  Ledger l(tmp(), small());
  BOOST_REQUIRE(l.open().has_value());
  BOOST_TEST((l.last_entry() == tuple<RaftTerm, RaftIndex>{RaftTerm{2}, RaftIndex{200}}));
  BOOST_TEST(*l.term(RaftIndex{100}) == RaftTerm{1});
  check_entries(l, RaftIndex{1}, items);
  check_entries(l, RaftIndex{137}, vector<Item>(items.begin() + 136, items.end()));
  BOOST_TEST(l.verify().has_value());

  auto more = append_n(l, 20, RaftTerm{3}, 5000);
  items.insert(items.end(), more.begin(), more.end());
  check_entries(l, RaftIndex{1}, items);
  BOOST_TEST(l.verify().has_value());

  // An entry with an older term, or WAL bytes that don't match the command, is refused.
  auto old = flush(RaftTerm{2}, 10, 1);
  BOOST_TEST(!l.append(old.e, old.wal).has_value());
  auto wrong = flush(RaftTerm{3}, 10, 1);
  wrong.wal.pop_back();
  BOOST_TEST(!l.append(wrong.e, wrong.wal).has_value());
}

BOOST_AUTO_TEST_CASE(end_marker_and_full_file) {
  TmpDir base;
  auto const s = tick_size();
  auto const h = file_header_size();
  for (auto const extra : {size_t{4}, size_t{7}, size_t{8}, size_t{20}}) {
    Dir tmp(base, "tmp2");
    {
      Ledger l(tmp(), small(h + 3 * s + extra));
      BOOST_REQUIRE(l.open().has_value());
      for (int i = 0; i < 4; ++i)
        BOOST_REQUIRE(l.append(tick(RaftTerm{1}).e, {}).has_value());
      BOOST_TEST(l.file_count() == 2U);
      BOOST_TEST(l.verify().has_value());
    }
    auto files = ledger_files(tmp());
    BOOST_REQUIRE(files.size() == 2U);
    BOOST_TEST(files.at(1).filename().string() == "0000000000000004.ledger");
    auto       data  = read_file(files.at(0));
    uint32_t   magic = 0;
    std::memcpy(&magic, data.data() + h + 3 * s, std::min<size_t>(4, extra));
    if (extra >= kLedgerMarkerSize)
      BOOST_TEST(magic == kLedgerEndMagic);
    else
      BOOST_TEST(magic == 0U);

    Ledger l(tmp(), small(h + 3 * s + extra));
    BOOST_REQUIRE(l.open().has_value());
    BOOST_TEST(std::get<1>(l.last_entry()) == RaftIndex{4});
    BOOST_REQUIRE(l.append(tick(RaftTerm{1}).e, {}).has_value());
    BOOST_TEST(l.verify().has_value());
    auto e = l.entries(RaftIndex{1}, 10);
    BOOST_REQUIRE(e.has_value() && e->has_value());
    BOOST_TEST((*e)->size() == 5U);
  }
}

BOOST_AUTO_TEST_CASE(follower_appends_records) {
  TmpDir base;
  Dir leader_dir(base, "leader_dir3");
  Dir follower_dir(base, "follower_dir4");
  Ledger leader(leader_dir(), small());
  Ledger follower(follower_dir(), small(3000));  // its own file boundaries
  BOOST_REQUIRE(leader.open().has_value());
  BOOST_REQUIRE(follower.open().has_value());

  auto items = append_n(leader, 100, RaftTerm{1}, 1);
  auto more  = append_n(leader, 30, RaftTerm{2}, 500);
  items.insert(items.end(), more.begin(), more.end());

  replicate(leader, follower, RaftIndex{0});
  BOOST_TEST((follower.last_entry() == leader.last_entry()));
  check_entries(follower, RaftIndex{1}, items);
  BOOST_TEST(follower.verify().has_value());
  for (auto i = 1U; i <= 130U; ++i)
    BOOST_TEST((*leader.crc(RaftIndex{i}) == *follower.crc(RaftIndex{i})));

  // A batch limited by its bytes still carries one record.
  auto one = leader.records(RaftIndex{5}, 10, 1);
  BOOST_REQUIRE(one.has_value() && one->has_value());
  BOOST_TEST((*one)->count() == 1U);

  // No entry at prev, or another term there: no match.
  auto batch = leader.records(RaftIndex{131}, 10);
  BOOST_REQUIRE(batch.has_value() && batch->has_value());
  BOOST_TEST((*batch)->count() == 0U);
  auto tail  = append_n(leader, 5, RaftTerm{2}, 900);
  auto later = leader.records(RaftIndex{133}, 10);
  BOOST_REQUIRE(later.has_value() && later->has_value());
  auto r = follower.append_records(RaftIndex{132}, RaftTerm{2}, concat(**later));
  BOOST_REQUIRE(r.has_value());
  BOOST_TEST(!r->has_value());
  auto next = leader.records(RaftIndex{131}, 10);
  r         = follower.append_records(RaftIndex{130}, RaftTerm{1}, concat(**next));
  BOOST_REQUIRE(r.has_value());
  BOOST_TEST(!r->has_value());

  // Records the follower has: skipped.
  auto again = leader.records(RaftIndex{50}, 10);
  r          = follower.append_records(RaftIndex{49}, RaftTerm{1}, concat(**again));
  BOOST_REQUIRE(r.has_value() && r->has_value());
  BOOST_TEST(**r == RaftIndex{59});
  BOOST_TEST(std::get<1>(follower.last_entry()) == RaftIndex{130});

  // Records that are not whole, or don't start after prev: refused.
  auto bytes = concat(**next);
  bytes.pop_back();
  BOOST_TEST(!follower.append_records(RaftIndex{130}, RaftTerm{2}, bytes).has_value());
  BOOST_TEST(!follower.append_records(RaftIndex{129}, RaftTerm{2}, concat(**next)).has_value());

  // The rest; then a heartbeat (no records).
  r = follower.append_records(RaftIndex{130}, RaftTerm{2}, concat(**next));
  BOOST_REQUIRE(r.has_value() && r->has_value());
  BOOST_TEST(**r == RaftIndex{135});
  r = follower.append_records(RaftIndex{135}, RaftTerm{2}, Bytes{});
  BOOST_REQUIRE(r.has_value() && r->has_value());
  BOOST_TEST(**r == RaftIndex{135});
  items.insert(items.end(), tail.begin(), tail.end());
  check_entries(follower, RaftIndex{1}, items);
  BOOST_TEST(follower.verify().has_value());
}

BOOST_AUTO_TEST_CASE(conflicting_records_truncate) {
  TmpDir base;
  Dir a_dir(base, "a_dir5");
  Dir b_dir(base, "b_dir6");
  Dir f_dir(base, "f_dir7");
  Ledger a(a_dir(), small());
  Ledger b(b_dir(), small());
  Ledger f(f_dir(), small());
  BOOST_REQUIRE(a.open().has_value());
  BOOST_REQUIRE(b.open().has_value());
  BOOST_REQUIRE(f.open().has_value());

  // a: 40 entries of term 1; b: a's first 10, then 25 of its own with term 2.
  auto a_items = append_n(a, 40, RaftTerm{1}, 1);
  replicate(a, f, RaftIndex{0});
  BOOST_TEST(std::get<1>(f.last_entry()) == RaftIndex{40});

  auto first10 = a.records(RaftIndex{1}, 10);
  BOOST_REQUIRE(b.append_records(RaftIndex{0}, RaftTerm{0}, concat(**first10)).has_value());
  auto b_items = append_n(b, 25, RaftTerm{2}, 7000);

  replicate(b, f, RaftIndex{10});
  BOOST_TEST((f.last_entry() == tuple<RaftTerm, RaftIndex>{RaftTerm{2}, RaftIndex{35}}));
  check_entries(f, RaftIndex{1}, vector<Item>(a_items.begin(), a_items.begin() + 10));
  check_entries(f, RaftIndex{11}, b_items);
  BOOST_TEST(f.verify().has_value());

  f.close();
  Ledger g(f_dir(), small());
  BOOST_REQUIRE(g.open().has_value());
  BOOST_TEST((g.last_entry() == tuple<RaftTerm, RaftIndex>{RaftTerm{2}, RaftIndex{35}}));
  check_entries(g, RaftIndex{11}, b_items);
  BOOST_TEST(g.verify().has_value());
}

BOOST_AUTO_TEST_CASE(truncation) {
  TmpDir base;
  Dir tmp(base, "tmp8");
  Ledger l(tmp(), small());
  BOOST_REQUIRE(l.open().has_value());
  auto items = append_n(l, 100, RaftTerm{1}, 1);
  auto files = l.file_count();

  BOOST_REQUIRE(l.truncate_after(RaftIndex{37}).has_value());
  BOOST_TEST(std::get<1>(l.last_entry()) == RaftIndex{37});
  BOOST_TEST(l.file_count() < files);
  BOOST_TEST(l.verify().has_value());
  check_entries(l, RaftIndex{1}, vector<Item>(items.begin(), items.begin() + 37));

  // At a file boundary: the file holding idx ends with it.
  auto const start = l.compaction_point(RaftIndex{30});
  BOOST_REQUIRE(start > RaftIndex{1});
  BOOST_REQUIRE(l.truncate_after(start - 1U).has_value());
  BOOST_TEST(std::get<1>(l.last_entry()) == start - 1U);
  BOOST_TEST(l.verify().has_value());
  items.resize(INT(start) - 1U);

  auto more = append_n(l, 30, RaftTerm{2}, 3000);
  items.insert(items.end(), more.begin(), more.end());
  check_entries(l, RaftIndex{1}, items);
  BOOST_TEST(l.verify().has_value());
  l.close();

  Ledger m(tmp(), small());
  BOOST_REQUIRE(m.open().has_value());
  check_entries(m, RaftIndex{1}, items);
  BOOST_TEST(m.verify().has_value());

  // Everything (a fresh log), and nothing before the first entry.
  BOOST_REQUIRE(m.truncate_after(RaftIndex{0}).has_value());
  BOOST_TEST(std::get<1>(m.last_entry()) == RaftIndex{0});
  BOOST_TEST(m.file_count() == 0U);
  BOOST_REQUIRE(m.append(tick(RaftTerm{1}).e, {}).has_value());
  BOOST_TEST(m.verify().has_value());
}

BOOST_AUTO_TEST_CASE(torn_tail) {
  TmpDir base;
  Dir tmp(base, "tmp9");
  vector<Item> items;
  {
    Ledger l(tmp(), small(1U << 20U));
    BOOST_REQUIRE(l.open().has_value());
    items = append_n(l, 30, RaftTerm{1}, 1);
  }
  auto path         = ledger_files(tmp()).back();
  auto [offs, end]  = record_offsets(path);
  BOOST_REQUIRE(offs.size() == 30U);

  // The last record torn: the log ends before it.
  uint8_t const flip = 0xFF;
  write_at(path, end - 1, std::span<uint8_t const>(&flip, 1));
  {
    Ledger l(tmp(), small(1U << 20U));
    BOOST_REQUIRE(l.open().has_value());
    BOOST_TEST(std::get<1>(l.last_entry()) == RaftIndex{29});
    items.pop_back();
    auto more = append_n(l, 3, RaftTerm{1}, 100);
    items.insert(items.end(), more.begin(), more.end());
    check_entries(l, RaftIndex{1}, items);
    BOOST_TEST(l.verify().has_value());
  }

  // Garbage after the end: ignored, and overwritten by the next appends.
  std::tie(offs, end) = record_offsets(path);
  auto garbage        = random_bytes(5000, 42);
  write_at(path, end, garbage);
  {
    Ledger l(tmp(), small(1U << 20U));
    BOOST_REQUIRE(l.open().has_value());
    BOOST_TEST(std::get<1>(l.last_entry()) == RaftIndex{32});
    auto more = append_n(l, 3, RaftTerm{1}, 200);
    items.insert(items.end(), more.begin(), more.end());
    check_entries(l, RaftIndex{1}, items);
    BOOST_TEST(l.verify().has_value());
  }
  Ledger l(tmp(), small(1U << 20U));
  BOOST_REQUIRE(l.open().has_value());
  check_entries(l, RaftIndex{1}, items);
}

BOOST_AUTO_TEST_CASE(stale_record_does_not_chain) {
  TmpDir base;
  Dir tmp(base, "tmp10");
  Bytes  old11;
  {
    Ledger l(tmp(), small(1U << 20U));
    BOOST_REQUIRE(l.open().has_value());
    (void)append_n(l, 11, RaftTerm{1}, 1);
    auto b = l.records(RaftIndex{11}, 1);
    BOOST_REQUIRE(b.has_value() && b->has_value());
    old11 = concat(**b);

    // A new leader's entry 10 replaces the old one; the old 11 is written after it again, as a
    // stale write from before the truncation would be.
    BOOST_REQUIRE(l.truncate_after(RaftIndex{9}).has_value());
    BOOST_REQUIRE(l.append(tick(RaftTerm{2}).e, {}).has_value());
  }
  auto path        = ledger_files(tmp()).back();
  auto [offs, end] = record_offsets(path);
  BOOST_REQUIRE(offs.size() == 10U);
  write_at(path, end, old11);

  Ledger l(tmp(), small(1U << 20U));
  BOOST_REQUIRE(l.open().has_value());
  BOOST_TEST((l.last_entry() == tuple<RaftTerm, RaftIndex>{RaftTerm{2}, RaftIndex{10}}));
  BOOST_TEST(l.verify().has_value());
}

BOOST_AUTO_TEST_CASE(compaction) {
  TmpDir base;
  Dir tmp(base, "tmp11");
  Ledger l(tmp(), small());
  BOOST_REQUIRE(l.open().has_value());
  auto items = append_n(l, 100, RaftTerm{1}, 1);
  auto more  = append_n(l, 20, RaftTerm{2}, 500);
  items.insert(items.end(), more.begin(), more.end());

  auto const p = l.compaction_point(RaftIndex{50});
  BOOST_REQUIRE(p > RaftIndex{1});
  BOOST_REQUIRE(p <= RaftIndex{50});
  BOOST_TEST(l.compaction_point(p) == p);
  auto const crc_before = *l.crc(p - 1U);
  auto const files      = l.file_count();

  BOOST_REQUIRE(l.drop_before(p).has_value());
  BOOST_TEST(l.file_count() < files);
  BOOST_TEST(l.first_idx() == p);
  BOOST_TEST(*l.term(p - 1U) == RaftTerm{1});  // the first file's header
  BOOST_TEST(*l.term(p) == RaftTerm{1});
  BOOST_TEST((l.base_state() == state_after(items, INT(p) - 1U)));
  BOOST_TEST(!l.drop_before(p + 1U).has_value());  // not a file's first index
  BOOST_TEST((*l.crc(p - 1U) == crc_before));
  auto none = l.entries(p - 1U, 1);
  BOOST_REQUIRE(none.has_value());
  BOOST_TEST(!none->has_value());
  check_entries(l, p, slice(items, INT(p) - 1U));
  BOOST_TEST(l.verify().has_value());
  BOOST_TEST(!l.truncate_after(p - 2U).has_value());
  l.close();

  Ledger m(tmp(), small());
  BOOST_REQUIRE(m.open(p).has_value());
  BOOST_TEST(m.first_idx() == p);
  BOOST_TEST((*m.crc(p - 1U) == crc_before));
  BOOST_TEST((m.base_state() == state_after(items, INT(p) - 1U)));
  check_entries(m, p, slice(items, INT(p) - 1U));

  // A compaction that saved its state and crashed before deleting the files: open() deletes them.
  Dir t2(base, "t212");
  {
    Ledger n(t2(), small());
    BOOST_REQUIRE(n.open().has_value());
    (void)append_n(n, 100, RaftTerm{1}, 1);
  }
  Ledger n(t2(), small());
  BOOST_REQUIRE(n.open(p).has_value());
  BOOST_TEST(n.first_idx() == p);
  BOOST_TEST(n.verify().has_value());
}

BOOST_AUTO_TEST_CASE(term_file) {
  TmpDir base;
  auto   path = base() / "term";
  {
    TermFile t(path, false);
    BOOST_REQUIRE(t.open().has_value());
    BOOST_TEST((t.get() == TermVote{}));
    BOOST_TEST(*t.set_current_term(RaftTerm{3}) == RaftTerm{3});
    BOOST_TEST(*t.set_current_term(RaftTerm{2}) == RaftTerm{3});  // never back
    BOOST_TEST(!*t.vote(RaftTerm{2}, RaftNodeId{1}));              // not the current term
    BOOST_TEST(*t.vote(RaftTerm{3}, RaftNodeId{1}));
    BOOST_TEST(*t.vote(RaftTerm{3}, RaftNodeId{1}));               // the same vote
    BOOST_TEST(!*t.vote(RaftTerm{3}, RaftNodeId{2}));              // once per term
  }
  TermFile t(path, false);
  auto     tv = t.open();
  BOOST_REQUIRE(tv.has_value());
  BOOST_TEST((*tv == TermVote{.term = RaftTerm{3}, .voted_for = RaftNodeId{1}}));
  BOOST_TEST(*t.set_current_term(RaftTerm{4}) == RaftTerm{4});
  BOOST_TEST(!t.get().voted_for.has_value());  // a new term clears the vote
}

BOOST_AUTO_TEST_CASE(install_snapshot) {
  TmpDir base;
  Dir    leader_dir(base, "leader");
  Ledger leader(leader_dir(), small());
  BOOST_REQUIRE(leader.open().has_value());
  auto items = append_n(leader, 100, RaftTerm{3}, 1);
  auto p     = leader.compaction_point(RaftIndex{60});
  BOOST_REQUIRE(leader.drop_before(p).has_value());
  auto const file = *leader.snapshot_file();
  BOOST_TEST(file.first == p);
  auto const end = leader.compaction_point(RaftIndex{100}) == p ? RaftIndex{100}
                                                                 : leader.compaction_point(
                                                                       file.first + 1000U) - 1U;
  (void)end;

  // The follower had other entries; the leader's first file replaces them.
  Dir f_dir(base, "f");
  {
    Ledger f(f_dir(), small());
    BOOST_REQUIRE(f.open().has_value());
    (void)append_n(f, 50, RaftTerm{1}, 9000);
    auto info = install(leader, f);
    BOOST_TEST(f.first_idx() == p);
    BOOST_TEST(f.file_count() == 1U);
    BOOST_TEST((info.header.state == state_after(items, INT(p) - 1U)));
    BOOST_TEST((f.base_state() == state_after(items, INT(p) - 1U)));
    BOOST_TEST(*f.term(p - 1U) == RaftTerm{3});
    BOOST_TEST(info.first_segment.has_value());
    BOOST_TEST(std::get<1>(f.last_entry()) == info.last);
    check_entries(f, p, slice(items, INT(p) - 1U, INT(info.last)));

    // AppendEntries continue after the installed records.
    replicate(leader, f, info.last);
    check_entries(f, p, slice(items, INT(p) - 1U));
    BOOST_TEST(f.verify().has_value());
  }

  // A chunk out of order is refused.
  Dir    o_dir(base, "o");
  Ledger o(o_dir(), small());
  BOOST_REQUIRE(o.open().has_value());
  auto chunk = leader.read_snapshot(file.first, 0, 100);
  BOOST_REQUIRE(o.install_chunk(0, **chunk).has_value());
  BOOST_TEST(!o.install_chunk(200, **chunk).has_value());
  BOOST_TEST(!o.prepare_install().has_value());  // a record cut short

  // Crashed after the meta file named the install: open() finishes it.
  Dir g_dir(base, "g");
  {
    Ledger g(g_dir(), small());
    BOOST_REQUIRE(g.open().has_value());
    (void)append_n(g, 50, RaftTerm{1}, 9000);
    for (size_t off = 0; off < file.size; off += 4096)
      BOOST_REQUIRE(g.install_chunk(off,
                                    **leader.read_snapshot(
                                        file.first, off, std::min<size_t>(4096, file.size - off)))
                        .has_value());
    BOOST_REQUIRE(g.prepare_install().has_value());
  }
  {
    Ledger g(g_dir(), small());
    BOOST_REQUIRE(g.open(p, p).has_value());
    BOOST_TEST(g.first_idx() == p);
    BOOST_TEST(!fs::exists(g_dir() / ".install.ledger"));
    BOOST_TEST(g.verify().has_value());
  }

  // Crashed before the meta file named it: the install file is dropped, the old log stays.
  Dir h_dir(base, "h");
  {
    Ledger h(h_dir(), small());
    BOOST_REQUIRE(h.open().has_value());
    (void)append_n(h, 50, RaftTerm{1}, 9000);
    BOOST_REQUIRE(h.install_chunk(0, **leader.read_snapshot(file.first, 0, file.size)).has_value());
    BOOST_REQUIRE(h.prepare_install().has_value());
  }
  Ledger h(h_dir(), small());
  BOOST_REQUIRE(h.open().has_value());
  BOOST_TEST(h.first_idx() == RaftIndex{1});
  BOOST_TEST(std::get<1>(h.last_entry()) == RaftIndex{50});
  BOOST_TEST(!fs::exists(h_dir() / ".install.ledger"));
}

BOOST_AUTO_TEST_CASE(file_headers) {
  // Every file's header holds the state at the entry before it, through truncations too.
  TmpDir       base;
  Dir          d(base, "d");
  vector<Item> items;
  {
    Ledger l(d(), small());
    BOOST_REQUIRE(l.open().has_value());
    items = append_n(l, 80, RaftTerm{1}, 1);
    BOOST_REQUIRE(l.truncate_after(RaftIndex{57}).has_value());
    items.resize(57);
    auto more = append_n(l, 60, RaftTerm{2}, 4000);
    items.insert(items.end(), more.begin(), more.end());
  }
  for (auto const& path : ledger_files(d())) {
    auto h = decode_file_header(read_file(path));
    BOOST_REQUIRE(h.has_value());
    auto const first = h->first.first;
    BOOST_TEST((h->first.state == state_after(items, INT(first) - 1U)), path);
    if (first > RaftIndex{1})
      BOOST_TEST(h->first.prev_term == items.at(INT(first) - 2U).e.term, path);
  }
  Ledger l(d(), small());
  BOOST_REQUIRE(l.open().has_value());  // checks every header against the records before it
  BOOST_TEST(l.verify().has_value());
  check_entries(l, RaftIndex{1}, items);
}

BOOST_AUTO_TEST_CASE(window_and_release) {
  TmpDir base;
  Dir tmp(base, "tmp17");
  Ledger l(tmp(), small());
  BOOST_REQUIRE(l.open().has_value());
  auto items = append_n(l, 50, RaftTerm{1}, 1);
  check_entries(l, RaftIndex{20}, vector<Item>(items.begin() + 19, items.end()));
  l.release(RaftIndex{30});
  check_entries(l, RaftIndex{20}, vector<Item>(items.begin() + 19, items.end()));
  check_entries(l, RaftIndex{31}, vector<Item>(items.begin() + 30, items.end()));
  BOOST_REQUIRE(l.truncate_after(RaftIndex{40}).has_value());
  check_entries(l, RaftIndex{31}, vector<Item>(items.begin() + 30, items.begin() + 40));
}

BOOST_AUTO_TEST_CASE(sync_thread) {
  TmpDir base;
  {
    Dir tmp(base, "tmp18");
    auto   o = small();
    o.no_sync = false;
    o.sync    = true;
    Ledger                  l(tmp(), o);
    std::mutex              mtx;
    std::condition_variable cv;
    RaftIndex               seen{0};
    l.set_durable_callback([&](RaftIndex idx) {
      auto lk = std::unique_lock{mtx};
      seen    = std::max(seen, idx);
      cv.notify_all();
    });
    BOOST_REQUIRE(l.open().has_value());
    (void)append_n(l, 5, RaftTerm{1}, 1);
    auto lk = std::unique_lock{mtx};
    BOOST_TEST(cv.wait_for(lk, std::chrono::seconds(10), [&] { return seen == RaftIndex{5}; }));
    BOOST_TEST(l.durable_idx() == RaftIndex{5});
  }
  {
    Dir tmp(base, "tmp19");
    auto   o  = small();
    o.no_sync = false;
    Ledger l(tmp(), o);
    BOOST_REQUIRE(l.open().has_value());
    (void)append_n(l, 5, RaftTerm{1}, 1);
    BOOST_TEST(l.durable_idx() == RaftIndex{5});  // sync off: durable once appended
    BOOST_TEST(l.sync().has_value());
  }
}

BOOST_AUTO_TEST_CASE(spare_file) {
  TmpDir base;
  Dir tmp(base, "tmp20");
  auto   o = small();
  o.spare  = true;
  Ledger l(tmp(), o);
  BOOST_REQUIRE(l.open().has_value());
  auto const spare = tmp() / ".spare.ledger";
  auto const wait  = [&] {
    for (int i = 0; i < 500 && !fs::exists(spare); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    return fs::exists(spare);
  };
  BOOST_REQUIRE(wait());
  auto items = append_n(l, 5, RaftTerm{1}, 1);  // the first file: the spare
  BOOST_REQUIRE(wait());
  auto more = append_n(l, 60, RaftTerm{1}, 100);
  items.insert(items.end(), more.begin(), more.end());
  BOOST_TEST(l.file_count() > 2U);
  BOOST_REQUIRE(wait());
  BOOST_TEST(fs::file_size(spare) == 4096U);
  check_entries(l, RaftIndex{1}, items);
  BOOST_TEST(l.verify().has_value());
}

namespace {

/// Fails the |fail_at|th write call (pwrite, datasync, rename) of the test's thread.
struct FaultLedger : public Ledger {
  using Ledger::Ledger;

  std::thread::id const owner = std::this_thread::get_id();
  int                   fail_at;
  int                   calls = 0;

  bool fail() {
    if (std::this_thread::get_id() != owner)
      return false;
    return calls++ == fail_at;
  }
  ssize_t do_pwrite(int fd, void const* data, size_t size, off_t offset) override {
    if (fail()) {
      errno = EIO;
      return -1;
    }
    return Ledger::do_pwrite(fd, data, size, offset);
  }
  int do_datasync(int fd) override {
    if (fail()) {
      errno = EIO;
      return -1;
    }
    return Ledger::do_datasync(fd);
  }
  int do_rename(char const* from, char const* to) override {
    if (fail()) {
      errno = EIO;
      return -1;
    }
    return Ledger::do_rename(from, to);
  }
};

}  // namespace

BOOST_AUTO_TEST_CASE(write_failures) {
  TmpDir base;
  auto o    = small(2048);
  o.no_sync = false;  // the datasyncs are called (and can fail)
  for (int k = 0;; ++k) {
    Dir tmp(base, "tmp21");
    vector<Item> done;
    bool         failed = false;
    {
      FaultLedger l(tmp(), o);
      l.fail_at = k;
      BOOST_REQUIRE(l.open().has_value());
      for (uint32_t i = 0; i < 40; ++i) {
        auto item = flush(RaftTerm{1}, (i * 37) % 400, i);
        if (!l.append(item.e, item.wal)) {
          failed = true;
          break;
        }
        done.push_back(std::move(item));
      }
      if (failed) {
        auto item = tick(RaftTerm{1});
        BOOST_TEST(!l.append(item.e, {}).has_value());  // every later write fails
      }
    }
    Ledger l(tmp(), o);
    BOOST_REQUIRE(l.open().has_value());
    BOOST_TEST(l.verify().has_value());
    BOOST_TEST(std::get<1>(l.last_entry()) == RaftIndex{done.size()});
    if (!done.empty())
      check_entries(l, RaftIndex{1}, done);
    if (!failed)
      break;
  }
}

BOOST_AUTO_TEST_CASE(term_and_meta_files) {
  TmpDir base;
  Dir tmp(base, "tmp22");
  auto   term = tmp() / "term";
  auto   meta = tmp() / "meta";

  auto t = read_term_file(term);
  BOOST_REQUIRE(t.has_value());
  BOOST_TEST((*t == TermVote{}));
  auto m = read_meta_file(meta);
  BOOST_REQUIRE(m.has_value());
  BOOST_TEST((*m == LedgerMeta{}));

  TermVote tv{.term = RaftTerm{7}, .voted_for = RaftNodeId{2}};
  BOOST_REQUIRE(write_term_file(term, tv, false).has_value());
  t = read_term_file(term);
  BOOST_REQUIRE(t.has_value());
  BOOST_TEST((*t == tv));

  LedgerMeta lm;
  lm.state.log_idx       = RaftIndex{123};
  lm.state.needs_backup  = true;
  lm.state.wal_end       = LogSeqNumber{2, 0x3000000};
  lm.state.first_idx     = RaftIndex{40};
  lm.installing          = RaftIndex{40};
  BOOST_REQUIRE(write_meta_file(meta, lm, false).has_value());
  m = read_meta_file(meta);
  BOOST_REQUIRE(m.has_value());
  BOOST_TEST((*m == lm));
  BOOST_TEST(!fs::exists(tmp() / "meta.tmp"));

  // Any byte changed, or the file cut short: rejected.
  auto data = read_file(meta);
  for (size_t b = 0; b < data.size(); ++b) {
    auto bad = data;
    bad.at(b) ^= 0x01;
    {
      std::ofstream out(meta, std::ios::binary | std::ios::trunc);
      out.write(reinterpret_cast<char const*>(bad.data()), static_cast<std::streamsize>(bad.size()));
    }
    BOOST_TEST(!read_meta_file(meta).has_value());
  }
  {
    std::ofstream out(meta, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<char const*>(data.data()),
              static_cast<std::streamsize>(data.size() - 1));
  }
  BOOST_TEST(!read_meta_file(meta).has_value());
}
