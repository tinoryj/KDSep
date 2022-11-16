//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_file_meta.h"

#include <ostream>
#include <sstream>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
uint64_t DeltaLogFileMetaData::GetDeltaLogFileSize() const {
  return DeltaLogHeader::kSize_ + total_deltaLog_bytes_ +
         DeltaLogFooter::kSize_;
}

DeltaLogFileMetaData::DeltaLogFileMetaData(uint64_t prefix_tree_leaf_ID,
                                           uint64_t deltaLog_file_id,
                                           uint64_t total_deltaLog_count,
                                           uint64_t total_deltaLog_bytes)
    : prefix_tree_leaf_ID_(prefix_tree_leaf_ID),
      deltaLog_file_id_(deltaLog_file_id),
      total_deltaLog_count_(total_deltaLog_bytes),
      total_deltaLog_bytes_(total_deltaLog_bytes) {}

std::ostream& operator<<(std::ostream& os,
                         const DeltaLogFileMetaData& shared_meta) {
  os << "deltaLog_file_id: " << shared_meta.GetDeltaLogFileID()
     << " total_deltaLog_count: " << shared_meta.GetTotalDeltaLogCount()
     << " total_deltaLog_bytes: " << shared_meta.GetTotalDeltaLogBytes();

  return os;
}

}  // namespace ROCKSDB_NAMESPACE
