//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <iosfwd>
#include <string>

#include "db/delta/delta_constants.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class JSONWriter;
class Slice;
class Status;

class DeltaFileGarbage {
 public:
  DeltaFileGarbage() = default;

  DeltaFileGarbage(uint64_t delta_file_number, uint64_t garbage_delta_count,
                   uint64_t garbage_delta_bytes)
      : delta_file_number_(delta_file_number),
        garbage_delta_count_(garbage_delta_count),
        garbage_delta_bytes_(garbage_delta_bytes) {}

  uint64_t GetDeltaFileNumber() const { return delta_file_number_; }
  uint64_t GetGarbageDeltaCount() const { return garbage_delta_count_; }
  uint64_t GetGarbageDeltaBytes() const { return garbage_delta_bytes_; }

  void EncodeTo(std::string* output) const;
  Status DecodeFrom(Slice* input);

  std::string DebugString() const;
  std::string DebugJSON() const;

 private:
  enum CustomFieldTags : uint32_t;

  uint64_t delta_file_number_ = kInvalidDeltaFileNumber;
  uint64_t garbage_delta_count_ = 0;
  uint64_t garbage_delta_bytes_ = 0;
};

bool operator==(const DeltaFileGarbage& lhs, const DeltaFileGarbage& rhs);
bool operator!=(const DeltaFileGarbage& lhs, const DeltaFileGarbage& rhs);

std::ostream& operator<<(std::ostream& os,
                         const DeltaFileGarbage& delta_file_garbage);
JSONWriter& operator<<(JSONWriter& jw,
                       const DeltaFileGarbage& delta_file_garbage);

}  // namespace ROCKSDB_NAMESPACE
