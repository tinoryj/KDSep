//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <memory>

#include "db/delta/delta_log_format.h"
#include "rocksdb/slice.h"

#define MAX_HEADER_SIZE(a, b, c) (a > b ? (a > c ? a : c) : (b > c ? b : c))

namespace ROCKSDB_NAMESPACE {

class RandomAccessFileReader;
class Env;
class Statistics;
class Status;
class SystemClock;

/**
 * DeltaLogSequentialReader is a general purpose log stream reader
 * implementation. The actual job of reading from the device is implemented by
 * the RandomAccessFileReader interface.
 *
 * Please see DeltaLogWriter for details on the file and record layout.
 */

class DeltaLogSequentialReader {
 public:
  enum ReadLevel {
    kReadHeader,
    kReadHeaderKey,
    kReadHeaderKeyDelta,
  };

  // Create a reader that will return log records from "*file_reader".
  DeltaLogSequentialReader(
      std::unique_ptr<RandomAccessFileReader>&& file_reader, SystemClock* clock,
      Statistics* statistics);

  // No copying allowed
  DeltaLogSequentialReader(const DeltaLogSequentialReader&) = delete;
  DeltaLogSequentialReader& operator=(const DeltaLogSequentialReader&) = delete;

  ~DeltaLogSequentialReader();

  Status ReadHeader(DeltaLogHeader* header);

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input. The contents filled in
  // *record will only be valid until the next mutating operation on this
  // reader.
  // If delta_offset is non-null, return offset of the delta through it.
  Status ReadRecord(DeltaLogRecord* record, ReadLevel level = kReadHeader,
                    uint64_t* delta_offset = nullptr);

  Status ReadFooter(DeltaLogFooter* footer);

  void ResetNextByte() { next_byte_ = 0; }

  uint64_t GetNextByte() const { return next_byte_; }

 private:
  Status ReadSlice(uint64_t size, Slice* slice, char* buf);

  const std::unique_ptr<RandomAccessFileReader> file_;
  SystemClock* clock_;

  Statistics* statistics_;

  Slice buffer_;
  char header_buf_[MAX_HEADER_SIZE(DeltaLogHeader::kSize, DeltaLogFooter::kSize,
                                   DeltaLogRecord::kHeaderSize)];

  // which byte to read next
  uint64_t next_byte_;
};

}  // namespace ROCKSDB_NAMESPACE

#undef MAX_HEADER_SIZE