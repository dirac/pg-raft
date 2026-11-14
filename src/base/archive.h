#pragma once

// Binary Boost.Serialization archives without a std::locale.
//
// boost::archive::binary_oarchive / binary_iarchive build a std::locale in their constructor
// (basic_binary_oprimitive / basic_binary_iprimitive, compiled into libboost_serialization), even
// with no_codecvt: about 5% of a node's CPU, since an archive is built for every message and every
// raft log entry. These archives keep Boost's structure layer (basic_binary_oarchive /
// basic_binary_iarchive: class ids, versions, collection sizes) and supply the primitive layer
// themselves, the way basic_binary_[io]primitive does it: a value is its sizeof bytes, a string is
// its size (std::size_t) then its bytes, an array of bitwise-serializable values is one block. The
// bytes are therefore those of binary_[io]archive with no_header (archive_test checks it).
//
// RawOArchive writes into a std::streambuf or appends to a std::string; RawIArchive reads from a
// std::streambuf or from a memory range (an LMDB value, without copying it). Running out of input
// throws boost::archive::archive_exception (input_stream_error).

#include <boost/archive/basic_binary_iarchive.hpp>
#include <boost/archive/basic_binary_oarchive.hpp>
#include <boost/archive/detail/register_archive.hpp>
#include <boost/serialization/array_optimization.hpp>
#include <boost/serialization/array_wrapper.hpp>
#include <boost/serialization/is_bitwise_serializable.hpp>
#include <cstring>
#include <streambuf>
#include <string>

#include "src/base/crc32c.h"

class RawOArchive : public boost::archive::basic_binary_oarchive<RawOArchive> {
 public:
  explicit RawOArchive(std::streambuf& sb)
      : boost::archive::basic_binary_oarchive<RawOArchive>(boost::archive::no_header), sb_(&sb) {}
  explicit RawOArchive(std::string& out)
      : boost::archive::basic_binary_oarchive<RawOArchive>(boost::archive::no_header), str_(&out) {}
  /// Also updates *crc (crc32c()) with every byte written, while writing them.
  RawOArchive(std::string& out, uint32_t* crc)
      : boost::archive::basic_binary_oarchive<RawOArchive>(boost::archive::no_header),
        str_(&out),
        crc_(crc) {}

  // The primitive layer (what basic_binary_oprimitive does).
  template <class T>
  void save(T const& t) {
    save_binary(&t, sizeof(T));
  }
  void save(bool t) { save_binary(&t, sizeof(t)); }
  void save(std::string const& s) {
    auto l = static_cast<std::size_t>(s.size());
    save(l);
    save_binary(s.data(), l);
  }
  void save(char const* s) {
    auto l = std::strlen(s);
    save(l);
    save_binary(s, l);
  }

  void save_binary(void const* address, std::size_t count) {
    if (crc_)
      *crc_ = crc32c(*crc_, address, count);
    if (str_) {
      str_->append(static_cast<char const*>(address), count);
      return;
    }
    if (static_cast<std::size_t>(sb_->sputn(static_cast<char const*>(address),
                                            static_cast<std::streamsize>(count))) != count)
      boost::serialization::throw_exception(boost::archive::archive_exception(
          boost::archive::archive_exception::output_stream_error));
  }

  // An array of bitwise-serializable values is one block (as basic_binary_oprimitive).
  struct use_array_optimization {
    template <class T>
    struct apply : public boost::serialization::is_bitwise_serializable<T> {};
  };
  template <class ValueType>
  void save_array(boost::serialization::array_wrapper<ValueType> const& a, unsigned int) {
    save_binary(a.address(), a.count() * sizeof(ValueType));
  }

  // basic_binary_oarchive's overrides (class ids, versions, sizes), then the generic ones.
  template <class T>
  void save_override(T& t) {
    this->boost::archive::basic_binary_oarchive<RawOArchive>::save_override(t);
  }

 private:
  std::streambuf* sb_  = nullptr;
  std::string*    str_ = nullptr;
  uint32_t*       crc_ = nullptr;
};

class RawIArchive : public boost::archive::basic_binary_iarchive<RawIArchive> {
 public:
  explicit RawIArchive(std::streambuf& sb)
      : boost::archive::basic_binary_iarchive<RawIArchive>(boost::archive::no_header), sb_(&sb) {}
  RawIArchive(void const* data, std::size_t size)
      : boost::archive::basic_binary_iarchive<RawIArchive>(boost::archive::no_header),
        pos_(static_cast<char const*>(data)),
        end_(pos_ + size) {}

  template <class T>
  void load(T& t) {
    load_binary(&t, sizeof(T));
  }
  void load(bool& t) {
    // A bool is the byte 0 or 1; anything else is not a message of ours.
    unsigned char b = 0;
    load_binary(&b, sizeof(b));
    if (b > 1)
      boost::serialization::throw_exception(
          boost::archive::archive_exception(boost::archive::archive_exception::input_stream_error));
    t = b != 0;
  }
  void load(std::string& s) {
    std::size_t l = 0;
    load(l);
    if (!sb_ && l > static_cast<std::size_t>(end_ - pos_))  // before resizing to a bogus size
      boost::serialization::throw_exception(
          boost::archive::archive_exception(boost::archive::archive_exception::input_stream_error));
    s.resize(l);
    if (l > 0)
      load_binary(s.data(), l);
  }

  void load_binary(void* address, std::size_t count) {
    if (sb_) {
      if (static_cast<std::size_t>(sb_->sgetn(static_cast<char*>(address),
                                              static_cast<std::streamsize>(count))) == count)
        return;
    } else if (count <= static_cast<std::size_t>(end_ - pos_)) {
      std::memcpy(address, pos_, count);
      pos_ += count;
      return;
    }
    boost::serialization::throw_exception(
        boost::archive::archive_exception(boost::archive::archive_exception::input_stream_error));
  }

  struct use_array_optimization {
    template <class T>
    struct apply : public boost::serialization::is_bitwise_serializable<T> {};
  };
  template <class ValueType>
  void load_array(boost::serialization::array_wrapper<ValueType>& a, unsigned int) {
    load_binary(a.address(), a.count() * sizeof(ValueType));
  }

  template <class T>
  void load_override(T& t) {
    this->boost::archive::basic_binary_iarchive<RawIArchive>::load_override(t);
  }
  // What basic_binary_iarchive's (in its .ipp, not instantiated for this archive) does.
  void load_override(boost::archive::class_name_type& t) {
    std::string cn;
    load(cn);
    if (cn.size() > BOOST_SERIALIZATION_MAX_KEY_SIZE - 1)
      boost::serialization::throw_exception(
          boost::archive::archive_exception(boost::archive::archive_exception::invalid_class_name));
    std::memcpy(t, cn.data(), cn.size());
    t.t[cn.size()] = '\0';
  }

 private:
  std::streambuf* sb_  = nullptr;
  char const*     pos_ = nullptr;
  char const*     end_ = nullptr;
};

BOOST_SERIALIZATION_REGISTER_ARCHIVE(RawOArchive)
BOOST_SERIALIZATION_REGISTER_ARCHIVE(RawIArchive)
// Arrays of bitwise-serializable values (std::vector<uint32_t>, ...) as one block, as Boost's
// binary archives do; without it they go element by element, with an item version first.
BOOST_SERIALIZATION_USE_ARRAY_OPTIMIZATION(RawOArchive)
BOOST_SERIALIZATION_USE_ARRAY_OPTIMIZATION(RawIArchive)
