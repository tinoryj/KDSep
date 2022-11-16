//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstdint>
#include <iosfwd>
#include <string>

#include "db/deltaLog/deltaLog_constants.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

class DeltaLogFileAddition {
 public:
  DeltaLogFileAddition() = default;

  DeltaLogFileAddition(uint64_t deltaLog_file_id, uint64_t total_deltaLog_count,
                       uint64_t total_deltaLog_bytes)
      : deltaLog_file_id_(deltaLog_file_id),
        total_deltaLog_count_(total_deltaLog_count),
        total_deltaLog_bytes_(total_deltaLog_bytes) {}

  uint64_t GetDeltaLogFileID() const { return deltaLog_file_id_; }
  uint64_t GetTotalDeltaLogCount() const { return total_deltaLog_count_; }
  uint64_t GetTotalDeltaLogBytes() const { return total_deltaLog_bytes_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  enum CustomFieldTags : uint32_t;

  uint64_t deltaLog_file_id_ = kGCSelectedDeltaLogFileID;
  uint64_t total_deltaLog_count_ = 0;
  uint64_t total_deltaLog_bytes_ = 0;
};

bool operator==(const DeltaLogFileAddition& lhs,
                const DeltaLogFileAddition& rhs);
bool operator!=(const DeltaLogFileAddition& lhs,
                const DeltaLogFileAddition& rhs);

std::ostream& operator<<(std::ostream& os,
                         const DeltaLogFileAddition& deltaLog_file_addition);
JSONWriter& operator<<(JSONWriter& jw,
                       const DeltaLogFileAddition& deltaLog_file_addition);

}  // namespace ROCKSDB_NAMESPACE
