//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>

#include "file/random_access_file_reader.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Status;
struct ImmutableOptions;
struct FileOptions;
class HistogramImpl;
struct ReadOptions;
class Slice;
class FilePrefetchBuffer;
class DeltaLogContents;
class Statistics;

class DeltaLogFileReader {
 public:
  static Status Create(const ImmutableOptions& immutable_options,
                       const FileOptions& file_options,
                       uint32_t column_family_id,
                       HistogramImpl* deltaLog_file_read_hist,
                       uint64_t deltaLog_file_id,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       std::unique_ptr<DeltaLogFileReader>* reader);

  DeltaLogFileReader(const DeltaLogFileReader&) = delete;
  DeltaLogFileReader& operator=(const DeltaLogFileReader&) = delete;

  ~DeltaLogFileReader();

  Status GetDeltaLog(const ReadOptions& read_options, const Slice& user_key,
                     uint64_t deltaLog_file_id,
                     FilePrefetchBuffer* prefetch_buffer,
                     MemoryAllocator* allocator,
                     std::unique_ptr<DeltaLogContents>* result,
                     uint64_t* bytes_read) const;

  uint64_t GetFileSize() const { return file_size_; }

 private:
  DeltaLogFileReader(std::unique_ptr<RandomAccessFileReader>&& file_reader,
                     uint64_t file_size, SystemClock* clock,
                     Statistics* statistics);

  static Status OpenFile(const ImmutableOptions& immutable_options,
                         const FileOptions& file_opts,
                         HistogramImpl* deltaLog_file_read_hist,
                         uint64_t deltaLog_file_id,
                         const std::shared_ptr<IOTracer>& io_tracer,
                         uint64_t* file_size,
                         std::unique_ptr<RandomAccessFileReader>* file_reader);

  static Status ReadHeader(const RandomAccessFileReader* file_reader,
                           uint32_t column_family_id, Statistics* statistics);

  static Status ReadFooter(const RandomAccessFileReader* file_reader,
                           uint64_t file_size, Statistics* statistics);

  using Buffer = std::unique_ptr<char[]>;

  static Status ReadFromFile(const RandomAccessFileReader* file_reader,
                             uint64_t read_offset, size_t read_size,
                             Statistics* statistics, Slice* slice, Buffer* buf,
                             AlignedBuf* aligned_buf,
                             Env::IOPriority rate_limiter_priority);

  static Status VerifyDeltaLog(const Slice& record_slice, const Slice& user_key,
                               uint64_t value_size);

  static Status UncompressDeltaLogIfNeeded(
      const Slice& value_slice, MemoryAllocator* allocator, SystemClock* clock,
      Statistics* statistics, std::unique_ptr<DeltaLogContents>* result);

  std::unique_ptr<RandomAccessFileReader> file_reader_;
  uint64_t file_size_;
  SystemClock* clock_;
  Statistics* statistics_;
};

}  // namespace ROCKSDB_NAMESPACE
