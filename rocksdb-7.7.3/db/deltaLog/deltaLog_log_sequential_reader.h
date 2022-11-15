//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <memory>

#include "db/deltaLog/deltaLog_log_format.h"
#include "rocksdb/slice.h"

#define MAX_HEADER_SIZE(a, b, c) (a > b ? (a > c ? a : c) : (b > c ? b : c))

namespace ROCKSDB_NAMESPACE {

class RandomAccessFileReader;
class Env;
class Statistics;
class Status;
class SystemClock;

/**
 * DeltaLogLogSequentialReader is a general purpose log stream reader
 * implementation. The actual job of reading from the device is implemented by
 * the RandomAccessFileReader interface.
 *
 * Please see DeltaLogLogWriter for details on the file and record layout.
 */

class DeltaLogLogSequentialReader {
 public:
  enum ReadLevel {
    kReadHeader,
    kReadHeaderKey,
    kReadHeaderKeyDeltaLog,
  };

  // Create a reader that will return log records from "*file_reader".
  DeltaLogLogSequentialReader(
      std::unique_ptr<RandomAccessFileReader>&& file_reader, SystemClock* clock,
      Statistics* statistics);

  // No copying allowed
  DeltaLogLogSequentialReader(const DeltaLogLogSequentialReader&) = delete;
  DeltaLogLogSequentialReader& operator=(const DeltaLogLogSequentialReader&) =
      delete;

  ~DeltaLogLogSequentialReader();

  Status ReadHeader(DeltaLogHeader* header);

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input. The contents filled in
  // *record will only be valid until the next mutating operation on this
  // reader.
  // If deltaLog_offset is non-null, return offset of the deltaLog through it.
  Status ReadRecord(DeltaLogRecord* record, ReadLevel level = kReadHeader,
                    uint64_t* deltaLog_offset = nullptr);

  Status ReadFooter(DeltaLogFooter* footer);

  void ResetNextByte() { next_byte_ = 0; }

  uint64_t GetNextByte() const { return next_byte_; }

 private:
  Status ReadSlice(uint64_t size, Slice* slice, char* buf);

  const std::unique_ptr<RandomAccessFileReader> file_;
  SystemClock* clock_;

  Statistics* statistics_;

  Slice buffer_;
  char header_buf_[MAX_HEADER_SIZE(DeltaLogHeader::kSize_,
                                   DeltaLogFooter::kSize_,
                                   DeltaLogRecord::kHeaderSize_)];

  // which byte to read next
  uint64_t next_byte_;
};

}  // namespace ROCKSDB_NAMESPACE

#undef MAX_HEADER_SIZE