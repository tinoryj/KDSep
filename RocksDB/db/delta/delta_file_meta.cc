//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <ostream>
#include <sstream>

#include "db/delta/delta_file_meta.h"
#include "db/delta/delta_log_format.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
uint64_t SharedDeltaFileMetaData::GetDeltaFileSize() const {
  return DeltaLogHeader::kSize + total_delta_bytes_ + DeltaLogFooter::kSize;
}

std::string SharedDeltaFileMetaData::DebugString() const {
  std::ostringstream oss;
  oss << (*this);

  return oss.str();
}

std::ostream& operator<<(std::ostream& os,
                         const SharedDeltaFileMetaData& shared_meta) {
  os << "delta_file_number: " << shared_meta.GetDeltaFileNumber()
     << " total_delta_count: " << shared_meta.GetTotalDeltaCount()
     << " total_delta_bytes: " << shared_meta.GetTotalDeltaBytes()
     << " checksum_method: " << shared_meta.GetChecksumMethod()
     << " checksum_value: "
     << Slice(shared_meta.GetChecksumValue()).ToString(/* hex */ true);

  return os;
}

std::string DeltaFileMetaData::DebugString() const {
  std::ostringstream oss;
  oss << (*this);

  return oss.str();
}

std::ostream& operator<<(std::ostream& os, const DeltaFileMetaData& meta) {
  const auto& shared_meta = meta.GetSharedMeta();
  assert(shared_meta);
  os << (*shared_meta);

  os << " linked_ssts: {";
  for (uint64_t file_number : meta.GetLinkedSsts()) {
    os << ' ' << file_number;
  }
  os << " }";

  os << " garbage_delta_count: " << meta.GetGarbageDeltaCount()
     << " garbage_delta_bytes: " << meta.GetGarbageDeltaBytes();

  return os;
}

}  // namespace ROCKSDB_NAMESPACE
