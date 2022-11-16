//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_file_meta.h"

#include <ostream>
#include <sstream>

#include "db/deltaLog/deltaLog_log_format.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
uint64_t SharedDeltaLogFileMetaData::GetDeltaLogFileSize() const {
  return DeltaLogHeader::kSize_ + total_deltaLog_bytes_ +
         DeltaLogFooter::kSize_;
}

std::ostream& operator<<(std::ostream& os,
                         const SharedDeltaLogFileMetaData& shared_meta) {
  os << "deltaLog_file_id: " << shared_meta.GetDeltaLogFileID()
     << " total_deltaLog_count: " << shared_meta.GetTotalDeltaLogCount()
     << " total_deltaLog_bytes: " << shared_meta.GetTotalDeltaLogBytes();

  return os;
}

std::ostream& operator<<(std::ostream& os, const DeltaLogFileMetaData& meta) {
  const auto& shared_meta = meta.GetSharedMeta();
  assert(shared_meta);
  os << (*shared_meta);

  os << " garbage_deltaLog_count: " << meta.GetGarbageDeltaLogCount()
     << " garbage_deltaLog_bytes: " << meta.GetGarbageDeltaLogBytes();

  return os;
}

}  // namespace ROCKSDB_NAMESPACE
