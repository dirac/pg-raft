#include "src/net/data.h"

namespace {

/// Read-only streambuf over the pages of a Data.
class Istreambuf : public std::basic_streambuf<char> {
 public:
  using chunk_t = Data::chunk_t;

  // total_size = logical byte count; the last chunk may be partially used.
  Istreambuf(std::vector<chunk_t*> chunks, std::size_t total_size)
      : chunks_(std::move(chunks)), total_size_(total_size) {
    if (!chunks_.empty() && total_size_ > 0) {
      char* base = chunks_[0]->data();
      setg(base, base, base + chunk_len(0));
    } else {
      setg(nullptr, nullptr, nullptr);
    }
  }

 protected:
  int_type underflow() override {
    if (gptr() < egptr())
      return traits_type::to_int_type(*gptr());

    std::size_t next = cur_chunk_ + 1;
    if (next >= chunks_.size() || next * kPageSize >= total_size_)
      return traits_type::eof();

    cur_chunk_ = next;
    char* base = chunks_[cur_chunk_]->data();
    setg(base, base, base + chunk_len(cur_chunk_));
    return traits_type::to_int_type(*gptr());
  }

  std::streamsize xsgetn(char* s, std::streamsize n) override {
    std::streamsize total = 0;
    while (total < n) {
      if (gptr() == egptr() && underflow() == traits_type::eof())
        break;
      std::streamsize avail = egptr() - gptr();
      std::streamsize k     = std::min(avail, n - total);
      std::memcpy(s + total, gptr(), static_cast<std::size_t>(k));
      gbump(static_cast<int>(k));
      total += k;
    }
    return total;
  }

  std::streamsize showmanyc() override {
    auto consumed = (cur_chunk_ * kPageSize) + static_cast<std::size_t>(gptr() - eback());
    return static_cast<std::streamsize>(total_size_ - consumed);
  }

 private:
  std::size_t chunk_len(std::size_t idx) const {
    std::size_t start = idx * kPageSize;
    return std::min<size_t>(kPageSize, total_size_ - start);
  }

  std::vector<chunk_t*> chunks_;
  std::size_t           total_size_;
  std::size_t           cur_chunk_ = 0;
};

}  // namespace

Data::Data(uint32_t capacity) : capacity_(capacity) {
  while (capacity != 0) {
    add_block();
    capacity -= std::min(u4(front()->size()), capacity);
  }
}

Data::Data(char format) {
  overflow(format);
}

std::unique_ptr<std::basic_streambuf<char>> Data::istream() const {
  return std::make_unique<Istreambuf>(*this, capacity_);
}

std::vector<::boost::asio::const_buffer> Data::const_buffer() const {
  auto result = std::vector<::boost::asio::const_buffer>{};
  result.reserve(BufferBase::size() + 3U);

  size_t avail = capacity_;
  for (auto const& block : *this) {
    if (avail == 0)
      break;
    auto size = std::min(block->size(), avail);
    result.emplace_back(block->data(), size);
    avail -= size;
  }
  return result;
}

std::vector<::boost::asio::mutable_buffer> Data::mutable_buffer() {
  auto result = std::vector<::boost::asio::mutable_buffer>{};
  result.reserve(BufferBase::size() + 1U);

  size_t avail = capacity_;
  for (auto& block : *this) {
    if (avail == 0)
      break;
    auto size = std::min(block->size(), avail);
    result.emplace_back(block->data(), size);
    avail -= size;
  }
  return result;
}

void Data::pack() {
  assert(front()->at(0) == 't' || front()->at(0) == 'b');
  capacity_ = ((size() - 1U) * kPageSize) + (reinterpret_cast<std::uintptr_t>(StreamBase::pptr()) -
                                             reinterpret_cast<std::uintptr_t>(StreamBase::pbase()));
}

Data::int_type Data::overflow(Data::int_type ch) {
  if (traits_type::eq_int_type(ch, traits_type::eof()))
    return traits_type::eof();

  add_block();
  StreamBase::setp(back()->begin(), back()->end());

  *pptr() = traits_type::to_char_type(ch);
  pbump(1);
  return ch;
}
