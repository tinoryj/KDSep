//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <iosfwd>
#include <string>

#include "db/deltaLog/deltaLog_constants.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

class DeltaLogFileGarbage {
 public:
  DeltaLogFileGarbage() = default;

  DeltaLogFileGarbage(uint64_t deltaLog_file_id,
                      uint64_t garbage_deltaLog_count,
                      uint64_t garbage_deltaLog_bytes)
      : deltaLog_file_id_(deltaLog_file_id),
        garbage_deltaLog_count_(garbage_deltaLog_count),
        garbage_deltaLog_bytes_(garbage_deltaLog_bytes) {}

  uint64_t GetDeltaLogFileID() const { return deltaLog_file_id_; }
  uint64_t GetGarbageDeltaLogCount() const { return garbage_deltaLog_count_; }
  uint64_t GetGarbageDeltaLogBytes() const { return garbage_deltaLog_bytes_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  enum CustomFieldTags : uint32_t;

  uint64_t deltaLog_file_id_ = kGCSelectedDeltaLogFileID;
  uint64_t garbage_deltaLog_count_ = 0;
  uint64_t garbage_deltaLog_bytes_ = 0;
};

bool operator==(const DeltaLogFileGarbage& lhs, const DeltaLogFileGarbage& rhs);
bool operator!=(const DeltaLogFileGarbage& lhs, const DeltaLogFileGarbage& rhs);

std::ostream& operator<<(std::ostream& os,
                         const DeltaLogFileGarbage& deltaLog_file_garbage);
JSONWriter& operator<<(JSONWriter& jw,
                       const DeltaLogFileGarbage& deltaLog_file_garbage);

}  // namespace ROCKSDB_NAMESPACE
