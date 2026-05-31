#include "src/net/data.h"

namespace {
std::array<char, 4> const end_marker{'#', 'E', 'N', 'D'};
}  // namespace

Data::Data(uint32_t capacity) : capacity_(capacity) {
  while (capacity) {
    add_block();
    capacity -= std::min(u4(front()->size()), capacity);
  }
}

Data::Data(char format) : capacity_(1) {
  add_block();
  StreamBase::setp(back()->begin(), back()->end());
  StreamBase::pbump(1);

  *front()->begin() = format;
}

char Data::format() {
  assert(consumed_ == 0);

  char f = *front()->begin();
  ++consumed_;

  return f;
}

/// Called from the completion handler for the async_write_some.
bool Data::consumed(size_t& bytes) {
  size_t length = std::min(bytes, capacity_ - consumed_);

  bytes -= length;
  consumed_ += length;

  // Delete a block?
  if (capacity_ && consumed_ >= std::min<size_t>(kPageSize, capacity_)) {
    del_block(begin());
    consumed_ -= std::min<size_t>(kPageSize, capacity_);
    capacity_ -= std::min<size_t>(kPageSize, capacity_);
  }
  return capacity_ == consumed_;
}

/// Get the buffers to async_write_some
boost::container::static_vector<::boost::asio::const_buffer, 2> Data::const_buffer() const {
  auto result = boost::container::static_vector<::boost::asio::const_buffer, 2>{};

  size_t consumed = consumed_;
  size_t avail    = capacity_ - consumed_;

  for (auto const& block : *this) {
    // Skip the data is already consumed
    if (consumed >= block->size())
      consumed -= block->size();
    else {
      auto size = std::min(block->size() - consumed, avail);
      result.emplace_back(block->data() + consumed, size);

      consumed = 0;
      avail -= size;
    }
    if (result.size() == result.capacity())
      return result;
  }
  return result;
}

/// Called from the completion handler for the async_read_some.
bool Data::filled(size_t length) {
  filled_ += length;
  assert(filled_ <= capacity_);

  return filled_ >= capacity_;
}

/// Get the buffers to async_read_some
boost::container::static_vector<::boost::asio::mutable_buffer, 2> Data::mutable_buffer() {
  auto result = boost::container::static_vector<::boost::asio::mutable_buffer, 2>{};

  size_t filled = filled_;
  size_t avail  = capacity_ - filled_;

  for (auto& block : *this) {
    if (filled >= block->size()) {
      filled -= block->size();
    } else {
      auto size = std::min(block->size() - filled, avail);
      result.emplace_back(block->data() + filled, size);

      filled = 0;
      avail -= size;

      if (result.size() == result.capacity())
        return result;
    }
  }
  return result;
}

void Data::pack() {
  assert(front()->at(0) == 't' || front()->at(0) == 'b');

  for (auto ch : end_marker)
    sputc(ch);

  capacity_ = filled_ =
      (size() - 1U) * kPageSize + (reinterpret_cast<std::uintptr_t>(StreamBase::pptr()) -
                                   reinterpret_cast<std::uintptr_t>(StreamBase::pbase()));
}

Data::int_type Data::overflow(Data::int_type ch) {
  if (traits_type::eq_int_type(ch, traits_type::eof()))
    return traits_type::eof();

  add_block();
  StreamBase::setp(back()->begin(), back()->end());

  auto c = traits_type::to_char_type(ch);
  StreamBase::xsputn(&c, 1);
  return ch;
}

Data::int_type Data::underflow() {
  if (consumed_ >= capacity_)
    return traits_type::eof();

  while (consumed_ >= front()->size()) {
    consumed_ -= front()->size();
    capacity_ -= front()->size();
    del_block(begin());
  }

  auto* gbeg  = front()->data() + consumed_;
  auto* gcurr = gbeg;
  auto* gend  = gbeg + std::min(front()->size() - consumed_, capacity_ - consumed_);

  StreamBase::setg(gbeg, gcurr, gend);
  consumed_ += reinterpret_cast<std::uintptr_t>(gend) - reinterpret_cast<std::uintptr_t>(gbeg);

  return static_cast<int_type>(*gcurr);
}

void Data::consume_all(Bytes& bytes) {
  boost::container::static_vector<char, end_marker.size()> end;
  while (end.size() != end_marker.size() ||
         memcmp(end.data(), end_marker.data(), end.size()) != 0) {
    if (end.size() == end_marker.size())
      end.erase(end.begin());
    end.emplace_back(static_cast<char>(sbumpc()));
  }

  do {
    auto n = reinterpret_cast<std::uintptr_t>(egptr()) - reinterpret_cast<std::uintptr_t>(gptr());
    bytes.insert(bytes.end(), gptr(), gptr() + n);
    gbump(i4(n));
  } while (underflow() != traits_type::eof());
}
