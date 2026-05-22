#include "src/wal_sender/wal_file_reader.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <thread>

WalFileReader::WalFileReader(std::string wal_dir, uint32_t timeline,
                             uint32_t wal_segment_size, int poll_ms)
    : wal_dir_(std::move(wal_dir)),
      timeline_(timeline),
      wal_segment_size_(wal_segment_size),
      poll_ms_(poll_ms) {}

void WalFileReader::stop() {
  stopped_.store(true, std::memory_order_relaxed);
}

// WAL segment filename:  TLI(8) + high32(lsn)(8) + seg_within_xlogid(8)
// where seg_within_xlogid = (uint32(lsn) / wal_segment_size) % segs_per_xlogid
// and   segs_per_xlogid   = 0x100000000 / wal_segment_size
std::string WalFileReader::segment_filename(uint64_t lsn) const {
  uint32_t high = static_cast<uint32_t>(lsn >> 32);
  uint32_t low  = static_cast<uint32_t>(lsn) / wal_segment_size_;
  char buf[32];
  snprintf(buf, sizeof(buf), "%08X%08X%08X", timeline_, high, low);
  return buf;
}

// WAL pages are 8kB; each page header starts with a 2-byte magic number.
// PG17 magic = 0xD116.  If we read a page whose magic doesn't match, the
// primary hasn't written it yet — we must wait rather than send zeros.
static constexpr uint32_t kPageSize   = 8192;
static constexpr uint16_t kXlogMagic = 0xD116;

void WalFileReader::run(uint64_t start_lsn, SegmentCallback cb) {
  uint64_t cur_lsn = start_lsn;

  while (!stopped_.load(std::memory_order_relaxed)) {
    std::string path = wal_dir_ + "/" + segment_filename(cur_lsn);

    // Wait for the segment file to appear.
    while (!stopped_.load(std::memory_order_relaxed) &&
           access(path.c_str(), R_OK) != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms_));
    }
    if (stopped_.load(std::memory_order_relaxed)) break;

    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) break;

    uint32_t seg_offset = static_cast<uint32_t>(cur_lsn % wal_segment_size_);
    if (lseek(fd, static_cast<off_t>(seg_offset), SEEK_SET) < 0) {
      close(fd);
      break;
    }

    // Read to the end of this segment, one page at a time.
    while (!stopped_.load(std::memory_order_relaxed)) {
      uint32_t page_off = static_cast<uint32_t>(cur_lsn % wal_segment_size_);
      bool at_page_boundary = (page_off % kPageSize == 0);

      // At every page boundary, verify the magic number before reading data.
      // fd position == page_off within the file at this point.
      if (at_page_boundary) {
        uint8_t magic_buf[2] = {0, 0};
        ssize_t nm = read(fd, magic_buf, 2);
        uint32_t magic = static_cast<uint32_t>(magic_buf[0]) |
                         (static_cast<uint32_t>(magic_buf[1]) << 8);
        bool ok = (nm == 2) && magic == kXlogMagic;
        if (!ok) {
          // If the primary did a WAL segment switch, the next segment will
          // exist while this page stays zeros forever.  Skip ahead.
          uint64_t seg_no = cur_lsn / static_cast<uint64_t>(wal_segment_size_);
          uint64_t next_seg_start =
              (seg_no + 1) * static_cast<uint64_t>(wal_segment_size_);
          std::string next_path =
              wal_dir_ + "/" + segment_filename(next_seg_start);
          if (access(next_path.c_str(), R_OK) == 0) {
            cur_lsn = next_seg_start;
            break;  // inner loop; outer loop will open next segment
          }
          lseek(fd, static_cast<off_t>(page_off), SEEK_SET);
          std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms_));
          continue;
        }
        lseek(fd, -static_cast<off_t>(nm), SEEK_CUR);
      }

      // Read up to one full page from the current position.
      uint32_t offset_in_page = page_off % kPageSize;
      uint32_t bytes_left_in_page = kPageSize - offset_in_page;
      uint32_t bytes_left_in_seg = wal_segment_size_ - page_off;
      size_t to_read = std::min(static_cast<size_t>(bytes_left_in_page),
                                static_cast<size_t>(bytes_left_in_seg));

      std::vector<uint8_t> buf(to_read);
      ssize_t n = read(fd, buf.data(), to_read);

      if (n > 0) {
        buf.resize(static_cast<size_t>(n));
        uint64_t seg_lsn = cur_lsn;
        cur_lsn += static_cast<uint64_t>(n);
        cb(WalSegment{seg_lsn, std::move(buf)});

        // Reached end of this segment: advance to the next file.
        if (cur_lsn % wal_segment_size_ == 0) break;
      } else {
        // EOF – primary hasn't written more yet; poll.
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms_));
      }
    }

    close(fd);
  }
}
