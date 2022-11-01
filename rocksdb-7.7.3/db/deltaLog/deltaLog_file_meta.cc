//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <ostream>
#include <sstream>

#include "db/deltaLog/deltaLog_file_meta.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
uint64_t SharedDeltaLogFileMetaData::GetDeltaLogFileSize() const {
  return DeltaLogLogHeader::kSize + total_deltaLog_bytes_ +
         DeltaLogLogFooter::kSize;
}

std::string SharedDeltaLogFileMetaData::DebugString() const {
  std::ostringstream oss;
  oss << (*this);

  return oss.str();
}

std::ostream& operator<<(std::ostream& os,
                         const SharedDeltaLogFileMetaData& shared_meta) {
  os << "deltaLog_file_number: " << shared_meta.GetDeltaLogFileNumber()
     << " total_deltaLog_count: " << shared_meta.GetTotalDeltaLogCount()
     << " total_deltaLog_bytes: " << shared_meta.GetTotalDeltaLogBytes()
     << " checksum_method: " << shared_meta.GetChecksumMethod()
     << " checksum_value: "
     << Slice(shared_meta.GetChecksumValue()).ToString(/* hex */ true);

  return os;
}

std::string DeltaLogFileMetaData::DebugString() const {
  std::ostringstream oss;
  oss << (*this);

  return oss.str();
}

std::ostream& operator<<(std::ostream& os, const DeltaLogFileMetaData& meta) {
  const auto& shared_meta = meta.GetSharedMeta();
  assert(shared_meta);
  os << (*shared_meta);

  os << " linked_ssts: {";
  for (uint64_t file_number : meta.GetLinkedSsts()) {
    os << ' ' << file_number;
  }
  os << " }";

  os << " garbage_deltaLog_count: " << meta.GetGarbageDeltaLogCount()
     << " garbage_deltaLog_bytes: " << meta.GetGarbageDeltaLogBytes();

  return os;
}

}  // namespace ROCKSDB_NAMESPACE
