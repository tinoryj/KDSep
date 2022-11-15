//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "db/deltaLog/deltaLog_log_format.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriter;
class SystemClock;
/**
 * DeltaLogLogWriter is the deltaLog log stream writer. It provides an
 * append-only abstraction for writing deltaLog data.
 *
 *
 * Look at deltaLog_db_format.h to see the details of the record formats.
 */

class DeltaLogLogWriter {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this DeltaLogLogWriter is in use.
  DeltaLogLogWriter(std::unique_ptr<WritableFileWriter>&& dest,
                    SystemClock* clock, Statistics* statistics,
                    uint64_t log_number, bool use_fsync, bool do_flush,
                    uint64_t boffset = 0);
  // No copying allowed
  DeltaLogLogWriter(const DeltaLogLogWriter&) = delete;
  DeltaLogLogWriter& operator=(const DeltaLogLogWriter&) = delete;

  ~DeltaLogLogWriter();

  static void ConstructDeltaLogHeader(std::string* buf, const Slice& key,
                                      const Slice& val, bool is_anchor);

  Status AddRecord(const Slice& key, const Slice& val, bool is_anchor);

  Status EmitPhysicalRecord(const std::string& headerbuf, const Slice& key,
                            const Slice& val);

  Status AppendFooter(DeltaLogFooter& footer);

  Status WriteHeader(DeltaLogHeader& header);

  WritableFileWriter* file() { return dest_.get(); }

  const WritableFileWriter* file() const { return dest_.get(); }

  uint64_t get_log_number() const { return log_number_; }

  Status Sync();

 private:
  std::unique_ptr<WritableFileWriter> dest_;
  SystemClock* clock_;
  Statistics* statistics_;
  uint64_t log_number_;
  uint64_t block_offset_;  // Current offset in block
  bool use_fsync_;
  bool do_flush_;

 public:
  enum ElemType { kEtNone, kEtFileHdr, kEtRecord, kEtFileFooter };
  ElemType last_elem_type_;
};

}  // namespace ROCKSDB_NAMESPACE
