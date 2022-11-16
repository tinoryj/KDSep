//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_set>

#include "db/deltaLog/deltaLog_log_format.h"
#include "rocksdb/rocksdb_namespace.h"
namespace ROCKSDB_NAMESPACE {

// DeltaLogFileMetaData represents the immutable part of deltaLog files'
// metadata, like the deltaLog file number, total number and size of deltaLogs,
// and value. There is supposed to be one object of this class per deltaLog file
// (shared across all versions that include the deltaLog file in question);
// hence, the type is neither copyable nor movable. A deltaLog file can be
// marked obsolete when the corresponding DeltaLogFileMetaData object is
// destroyed.

class DeltaLogFileMetaData {
 public:
  static std::shared_ptr<DeltaLogFileMetaData> Create(
      uint64_t prefix_tree_leaf_ID, uint64_t deltaLog_file_id,
      uint64_t total_deltaLog_count, uint64_t total_deltaLog_bytes) {
    return std::shared_ptr<DeltaLogFileMetaData>(
        new DeltaLogFileMetaData(prefix_tree_leaf_ID, deltaLog_file_id,
                                 total_deltaLog_count, total_deltaLog_bytes));
  }

  DeltaLogFileMetaData(uint64_t prefix_tree_leaf_ID, uint64_t deltaLog_file_id,
                       uint64_t total_deltaLog_count,
                       uint64_t total_deltaLog_bytes);
  DeltaLogFileMetaData& operator=(const DeltaLogFileMetaData&) = delete;

  uint64_t GetDeltaLogFileSize() const;
  uint64_t GetDeltaLogFileID() const { return deltaLog_file_id_; }
  uint64_t GetTotalDeltaLogCount() const { return total_deltaLog_count_; }
  uint64_t GetTotalDeltaLogBytes() const { return total_deltaLog_bytes_; }

 private:
  DeltaLogFileMetaData(uint64_t deltaLog_file_id, uint64_t total_deltaLog_count,
                       uint64_t total_deltaLog_bytes)
      : deltaLog_file_id_(deltaLog_file_id),
        total_deltaLog_count_(total_deltaLog_count),
        total_deltaLog_bytes_(total_deltaLog_bytes) {}

  uint64_t prefix_tree_leaf_ID_;
  uint64_t deltaLog_file_id_;
  uint64_t total_deltaLog_count_;
  uint64_t total_deltaLog_bytes_;
};

std::ostream& operator<<(std::ostream& os,
                         const DeltaLogFileMetaData& shared_meta);

}  // namespace ROCKSDB_NAMESPACE
