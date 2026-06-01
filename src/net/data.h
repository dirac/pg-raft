#pragma once

#include "src/base/base.h"

constexpr unsigned kPageSize = 4096 * 2;

struct Data : public std::basic_streambuf<char>, public vector<std::array<char, kPageSize>*> {
  using StreamBase  = std::basic_streambuf<char>;
  using char_type   = typename StreamBase::char_type;
  using int_type    = typename StreamBase::int_type;
  using traits_type = typename std::char_traits<char_type>;

  using BufferBase = vector<std::array<char, kPageSize>*>;

  Data(Data const&)            = delete;
  Data& operator=(Data const&) = delete;

  Data(Data&&)            = default;
  Data& operator=(Data&&) = default;

  /// Construct a mutable message to write to with the given capacity.
  Data(uint32_t capacity = 0U);

  ~Data() {
    while (!empty())
      del_block(begin());
  }

  /// Called from the completion handler for the async_read_some.
  /// Returns true if the buffers are full.
  bool filled(size_t length);

  /// Called to get buffers for async_read_some.
  boost::container::static_vector<::boost::asio::mutable_buffer, 2> mutable_buffer();

  /// Called from the completion handler for the async_write_some.
  /// Returns true if the buffers have been consumed to the full.
  bool consumed(size_t& length);

  /// Called to get buffers for async_write_some.
  boost::container::static_vector<::boost::asio::const_buffer, 2> const_buffer() const;

  // The size of the buffer.
  size_t capacity() const { return capacity_; }

 protected:
  int_type overflow(int_type ch) final;
  int_type underflow() final;

  // Updates the filled_ and capacity_ position.
  // flush is called after serializing the object, not it can be used to read the object back.
  void pack();

  char format();

  void consume_all(Bytes& bytes);

  /// Construct a buffer to store data of the given format. Used during serialization.
  Data(char format);

  template <typename T>
  friend Data to_data(T const& t, bool binary);

  template <typename T>
  friend Result<T> from_data(Data msg, Bytes& bytes);

 private:
  size_t filled_{0};
  size_t consumed_{0};
  size_t capacity_;

  void add_block() {
    emplace_back(reinterpret_cast<std::array<char, kPageSize>*>(MemoryPool<kPageSize>::malloc()));
  }
  void del_block(auto iter) {
    MemoryPool<kPageSize>::free(*iter);
    erase(iter);
  }
};

template <typename T>
Data to_data(T const& t, bool binary) {
  auto msg = Data{binary ? 'b' : 't'};
  auto str = std::basic_ostream{&msg};

  if (binary) {
    {
      auto oa = boost::archive::binary_oarchive{str, boost::archive::no_header};
      oa << t;
    }
  } else {
    {
      auto oa = boost::archive::text_oarchive{str};
      oa << t;
    }
  }
  msg.pack();
  return msg;
}

template <typename T>
Data to_data(T const& t) {
  return to_data<T>(t, /*binary=*/true);
}

template <typename T>
Result<T> from_data(Data msg, Bytes& bytes) {
  // Get the format.
  char f = msg.format();

  T t;

  std::basic_istream str{&msg};
  if (f == 'b') {
    boost::archive::binary_iarchive ia(str, boost::archive::no_header);
    ia >> t;
  } else {
    boost::archive::text_iarchive ia(str);
    ia >> t;
  }
  msg.consume_all(bytes);
  return t;
}

template <typename T>
Result<T> from_data(Data msg) {
  Bytes bytes;
  auto  t = from_data<T>(std::move(msg), bytes);
  assert(bytes.empty());
  return t;
}
