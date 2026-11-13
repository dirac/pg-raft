#pragma once

#include "src/base/archive.h"
#include "src/base/base.h"

constexpr unsigned kPageSize = 4096 * 2;

/// Pool-backed buffer of kPageSize pages. Objects are serialized into it with to_data() (it is the
/// streambuf of the output archive) and deserialized with from_data(). The connection reads and
/// writes it as scatter/gather buffers.
struct Data : public std::basic_streambuf<char>, public vector<std::array<char, kPageSize>*> {
  using StreamBase = std::basic_streambuf<char>;
  using StreamBase::char_type;
  using StreamBase::int_type;
  using traits_type = std::char_traits<char_type>;

  using chunk_t    = std::array<char, kPageSize>;
  using BufferBase = vector<chunk_t*>;

  Data(Data const&)            = delete;
  Data& operator=(Data const&) = delete;

  Data(Data&&)            = default;
  Data& operator=(Data&&) = default;

  /// Construct a buffer of the given size, to receive into.
  explicit Data(uint32_t capacity = 0U);

  ~Data() override {
    while (!empty())
      del_block(begin());
  }

  /// A streambuf to read the content.
  std::unique_ptr<std::basic_streambuf<char>> istream() const;

  /// The buffers to receive into.
  std::vector<::boost::asio::mutable_buffer> mutable_buffer();

  /// The buffers to send.
  std::vector<::boost::asio::const_buffer> const_buffer() const;

  /// The size of the content.
  size_t capacity() const { return capacity_; }

 protected:
  int_type overflow(int_type ch) final;

  /// Called after serializing the object: sets the size of the content.
  void pack();

  /// Construct a buffer to store data of the given format ('b' or 't'). Used by to_data().
  explicit Data(char format);

  template <typename T>
  friend Data to_data(T const& t, bool binary);

 private:
  size_t capacity_{0};

  void add_block() { emplace_back(reinterpret_cast<chunk_t*>(MemoryPool<kPageSize>::malloc())); }
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
    // RawOArchive: binary_oarchive's bytes (no_header), without building a std::locale per message.
    auto oa = RawOArchive{msg};
    oa << t;
  } else {
    auto oa = boost::archive::text_oarchive{str};
    oa << t;
  }
  msg.pack();
  return msg;
}

template <typename T>
Data to_data(T const& t) {
  return to_data<T>(t, /*binary=*/true);
}

/// Deserialize. A malformed message is an EBADMSG error.
template <typename T>
Result<T> from_data(Data const& msg) {
  auto istream = msg.istream();
  auto str     = std::basic_istream{istream.get()};

  try {
    T t;
    switch (str.get()) {
      case 'b': {
        RawIArchive ia(*str.rdbuf());
        ia >> t;
      } break;

      case 't': {
        boost::archive::text_iarchive ia(str);
        ia >> t;
      } break;

      default:
        return system_error_code(EBADMSG);
    }
    return t;
  } catch (std::exception const&) {
    return system_error_code(EBADMSG);
  }
}
