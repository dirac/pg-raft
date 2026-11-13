// The part of Boost's archive templates that libboost_serialization compiles only for its own
// archives: the serializer maps, instantiated here for RawOArchive / RawIArchive
// (src/base/archive.h). basic_binary_[io]archive's .ipp members are not needed: init() reads or
// writes the header, which these archives never have, and RawIArchive overrides the class-name
// load itself.

#include "src/base/archive.h"

#include <boost/archive/impl/archive_serializer_map.ipp>

template class boost::archive::detail::archive_serializer_map<RawOArchive>;
template class boost::archive::detail::archive_serializer_map<RawIArchive>;
