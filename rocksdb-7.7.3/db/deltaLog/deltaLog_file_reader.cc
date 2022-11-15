//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_file_reader.h"

#include <cassert>
#include <string>

#include "db/deltaLog/deltaLog_contents.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "file/file_prefetch_buffer.h"
#include "file/filename.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/multiget_context.h"
#include "test_util/sync_point.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

Status DeltaLogFileReader::Create(
    const ImmutableOptions& immutable_options, const FileOptions& file_options,
    uint32_t column_family_id, HistogramImpl* deltaLog_file_read_hist,
    uint64_t deltaLog_file_id, const std::shared_ptr<IOTracer>& io_tracer,
    std::unique_ptr<DeltaLogFileReader>* deltaLog_file_reader) {
  assert(deltaLog_file_reader);
  assert(!*deltaLog_file_reader);

  uint64_t file_size = 0;
  std::unique_ptr<RandomAccessFileReader> file_reader;

  {
    const Status s =
        OpenFile(immutable_options, file_options, deltaLog_file_read_hist,
                 deltaLog_file_id, io_tracer, &file_size, &file_reader);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file_reader);

  Statistics* const statistics = immutable_options.stats;

  {
    const Status s =
        ReadHeader(file_reader.get(), column_family_id, statistics);
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s = ReadFooter(file_reader.get(), file_size, statistics);
    if (!s.ok()) {
      return s;
    }
  }

  deltaLog_file_reader->reset(new DeltaLogFileReader(
      std::move(file_reader), file_size, immutable_options.clock, statistics));

  return Status::OK();
}

Status DeltaLogFileReader::OpenFile(
    const ImmutableOptions& immutable_options, const FileOptions& file_opts,
    HistogramImpl* deltaLog_file_read_hist, uint64_t deltaLog_file_id,
    const std::shared_ptr<IOTracer>& io_tracer, uint64_t* file_size,
    std::unique_ptr<RandomAccessFileReader>* file_reader) {
  assert(file_size);
  assert(file_reader);

  const auto& cf_paths = immutable_options.cf_paths;
  assert(!cf_paths.empty());

  const std::string deltaLog_file_path =
      DeltaLogFileName(cf_paths.front().path, deltaLog_file_id);

  FileSystem* const fs = immutable_options.fs.get();
  assert(fs);

  constexpr IODebugContext* dbg = nullptr;

  {
    TEST_SYNC_POINT("DeltaLogFileReader::OpenFile:GetFileSize");

    const Status s =
        fs->GetFileSize(deltaLog_file_path, IOOptions(), file_size, dbg);
    if (!s.ok()) {
      return s;
    }
  }

  if (*file_size < DeltaLogHeader::kSize_ + DeltaLogFooter::kSize_) {
    return Status::Corruption("Malformed deltaLog file");
  }

  std::unique_ptr<FSRandomAccessFile> file;

  {
    TEST_SYNC_POINT("DeltaLogFileReader::OpenFile:NewRandomAccessFile");

    const Status s =
        fs->NewRandomAccessFile(deltaLog_file_path, file_opts, &file, dbg);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file);

  if (immutable_options.advise_random_on_open) {
    file->Hint(FSRandomAccessFile::kRandom);
  }

  file_reader->reset(new RandomAccessFileReader(
      std::move(file), deltaLog_file_path, immutable_options.clock, io_tracer,
      immutable_options.stats, DELTALOG_DB_DELTALOG_FILE_READ_MICROS,
      deltaLog_file_read_hist, immutable_options.rate_limiter.get(),
      immutable_options.listeners));

  return Status::OK();
}

Status DeltaLogFileReader::ReadHeader(const RandomAccessFileReader* file_reader,
                                      uint32_t column_family_id,
                                      Statistics* statistics) {
  assert(file_reader);

  Slice header_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  {
    TEST_SYNC_POINT("DeltaLogFileReader::ReadHeader:ReadFromFile");

    constexpr uint64_t read_offset = 0;
    constexpr size_t read_size = DeltaLogHeader::kSize_;

    // TODO: rate limit reading headers from deltaLog files.
    const Status s = ReadFromFile(file_reader, read_offset, read_size,
                                  statistics, &header_slice, &buf, &aligned_buf,
                                  Env::IO_TOTAL /* rate_limiter_priority */);
    if (!s.ok()) {
      return s;
    }

    TEST_SYNC_POINT_CALLBACK("DeltaLogFileReader::ReadHeader:TamperWithResult",
                             &header_slice);
  }

  DeltaLogHeader header;

  {
    const Status s = header.DecodeFrom(header_slice);
    if (!s.ok()) {
      return s;
    }
  }

  if (header.column_family_id_ != column_family_id) {
    return Status::Corruption("Column family ID mismatch");
  }

  return Status::OK();
}

Status DeltaLogFileReader::ReadFooter(const RandomAccessFileReader* file_reader,
                                      uint64_t file_size,
                                      Statistics* statistics) {
  assert(file_size >= DeltaLogHeader::kSize_ + DeltaLogFooter::kSize_);
  assert(file_reader);

  Slice footer_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  {
    TEST_SYNC_POINT("DeltaLogFileReader::ReadFooter:ReadFromFile");

    const uint64_t read_offset = file_size - DeltaLogFooter::kSize_;
    constexpr size_t read_size = DeltaLogFooter::kSize_;

    // TODO: rate limit reading footers from deltaLog files.
    const Status s = ReadFromFile(file_reader, read_offset, read_size,
                                  statistics, &footer_slice, &buf, &aligned_buf,
                                  Env::IO_TOTAL /* rate_limiter_priority */);
    if (!s.ok()) {
      return s;
    }

    TEST_SYNC_POINT_CALLBACK("DeltaLogFileReader::ReadFooter:TamperWithResult",
                             &footer_slice);
  }

  DeltaLogFooter footer;

  {
    const Status s = footer.DecodeFrom(footer_slice);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status DeltaLogFileReader::ReadFromFile(
    const RandomAccessFileReader* file_reader, uint64_t read_offset,
    size_t read_size, Statistics* statistics, Slice* slice, Buffer* buf,
    AlignedBuf* aligned_buf, Env::IOPriority rate_limiter_priority) {
  assert(slice);
  assert(buf);
  assert(aligned_buf);

  assert(file_reader);

  RecordTick(statistics, DELTALOG_DB_DELTALOG_FILE_BYTES_READ, read_size);

  Status s;

  if (file_reader->use_direct_io()) {
    constexpr char* scratch = nullptr;

    s = file_reader->Read(IOOptions(), read_offset, read_size, slice, scratch,
                          aligned_buf, rate_limiter_priority);
  } else {
    buf->reset(new char[read_size]);
    constexpr AlignedBuf* aligned_scratch = nullptr;

    s = file_reader->Read(IOOptions(), read_offset, read_size, slice,
                          buf->get(), aligned_scratch, rate_limiter_priority);
  }

  if (!s.ok()) {
    return s;
  }

  if (slice->size() != read_size) {
    return Status::Corruption("Failed to read data from deltaLog file");
  }

  return Status::OK();
}

DeltaLogFileReader::DeltaLogFileReader(
    std::unique_ptr<RandomAccessFileReader>&& file_reader, uint64_t file_size,
    SystemClock* clock, Statistics* statistics)
    : file_reader_(std::move(file_reader)),
      file_size_(file_size),
      clock_(clock),
      statistics_(statistics) {
  assert(file_reader_);
}

DeltaLogFileReader::~DeltaLogFileReader() = default;

Status DeltaLogFileReader::GetDeltaLog(
    const ReadOptions& read_options, const Slice& user_key,
    uint64_t deltaLog_file_id, FilePrefetchBuffer* prefetch_buffer,
    MemoryAllocator* allocator, std::unique_ptr<DeltaLogContents>* result,
    uint64_t* bytes_read) const {
  assert(result);
  const uint64_t key_size = user_key.size();

  const uint64_t record_offset = 0;
  const uint64_t record_size = value_size;

  Slice record_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  bool prefetched = false;

  if (prefetch_buffer) {
    Status s;
    constexpr bool for_compaction = true;

    prefetched = prefetch_buffer->TryReadFromCache(
        IOOptions(), file_reader_.get(), record_offset,
        static_cast<size_t>(record_size), &record_slice, &s,
        read_options.rate_limiter_priority, for_compaction);
    if (!s.ok()) {
      return s;
    }
  }

  if (!prefetched) {
    TEST_SYNC_POINT("DeltaLogFileReader::GetDeltaLog:ReadFromFile");
    PERF_COUNTER_ADD(deltaLog_read_count, 1);
    PERF_COUNTER_ADD(deltaLog_read_byte, record_size);
    PERF_TIMER_GUARD(deltaLog_read_time);
    const Status s = ReadFromFile(file_reader_.get(), record_offset,
                                  static_cast<size_t>(record_size), statistics_,
                                  &record_slice, &buf, &aligned_buf,
                                  read_options.rate_limiter_priority);
    if (!s.ok()) {
      return s;
    }
  }

  TEST_SYNC_POINT_CALLBACK("DeltaLogFileReader::GetDeltaLog:TamperWithResult",
                           &record_slice);

  if (read_options.verify_checksums) {
    const Status s = VerifyDeltaLog(record_slice, user_key, value_size);
    if (!s.ok()) {
      return s;
    }
  }

  const Slice value_slice(record_slice.data(), value_size);

  CacheAllocationPtr allocation = AllocateBlock(value_slice.size(), allocator);
  memcpy(allocation.get(), value_slice.data(), value_slice.size());

  *result = DeltaLogContents::Create(std::move(allocation), value_slice.size());

  if (bytes_read) {
    *bytes_read = record_size;
  }

  return Status::OK();
}

Status DeltaLogFileReader::VerifyDeltaLog(const Slice& record_slice,
                                          const Slice& user_key,
                                          uint64_t value_size) {
  PERF_TIMER_GUARD(deltaLog_checksum_time);

  DeltaLogRecord record;

  const Slice header_slice(record_slice.data(), DeltaLogRecord::kHeaderSize_);

  {
    const Status s = record.DecodeHeaderFrom(header_slice);
    if (!s.ok()) {
      return s;
    }
  }

  if (record.key_size_ != user_key.size()) {
    return Status::Corruption("Key size mismatch when reading deltaLog");
  }

  if (record.value_size_ != value_size) {
    return Status::Corruption("Value size mismatch when reading deltaLog");
  }

  record.key_ = Slice(record_slice.data() + DeltaLogRecord::kHeaderSize_,
                      record.key_size_);
  if (record.key_ != user_key) {
    return Status::Corruption("Key mismatch when reading deltaLog");
  }

  record.value_ = Slice(record.key_.data() + record.key_size_, value_size);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
