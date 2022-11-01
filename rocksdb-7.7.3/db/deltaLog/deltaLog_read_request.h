//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>

#include "rocksdb/compression_type.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// A read DeltaLog request structure for use in DeltaLogSource::MultiGetDeltaLog
// and DeltaLogFileReader::MultiGetDeltaLog.
struct DeltaLogReadRequest {
  // User key to lookup the paired deltaLog
  const Slice* user_key = nullptr;

  // File offset in bytes
  uint64_t offset = 0;

  // Length to read in bytes
  size_t len = 0;

  // DeltaLog compression type
  CompressionType compression = kNoCompression;

  // Output parameter set by MultiGetDeltaLog() to point to the data buffer, and
  // the number of valid bytes
  PinnableSlice* result = nullptr;

  // Status of read
  Status* status = nullptr;

  DeltaLogReadRequest(const Slice& _user_key, uint64_t _offset, size_t _len,
                      CompressionType _compression, PinnableSlice* _result,
                      Status* _status)
      : user_key(&_user_key),
        offset(_offset),
        len(_len),
        compression(_compression),
        result(_result),
        status(_status) {}

  DeltaLogReadRequest() = default;
  DeltaLogReadRequest(const DeltaLogReadRequest& other) = default;
  DeltaLogReadRequest& operator=(const DeltaLogReadRequest& other) = default;
};

using DeltaLogFileReadRequests =
    std::tuple<uint64_t /* file_number */, uint64_t /* file_size */,
               autovector<DeltaLogReadRequest>>;

}  // namespace ROCKSDB_NAMESPACE
