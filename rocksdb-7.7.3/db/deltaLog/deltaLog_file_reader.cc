//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <string>

#include "db/deltaLog/deltaLog_contents.h"
#include "db/deltaLog/deltaLog_file_reader.h"
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
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

Status DeltaLogFileReader::Create(
    const ImmutableOptions& immutable_options, const FileOptions& file_options,
    uint32_t column_family_id, HistogramImpl* deltaLog_file_read_hist,
    uint64_t deltaLog_file_number, const std::shared_ptr<IOTracer>& io_tracer,
    std::unique_ptr<DeltaLogFileReader>* deltaLog_file_reader) {
  assert(deltaLog_file_reader);
  assert(!*deltaLog_file_reader);

  uint64_t file_size = 0;
  std::unique_ptr<RandomAccessFileReader> file_reader;

  {
    const Status s =
        OpenFile(immutable_options, file_options, deltaLog_file_read_hist,
                 deltaLog_file_number, io_tracer, &file_size, &file_reader);
    if (!s.ok()) {
      return s;
    }
  }

  assert(file_reader);

  Statistics* const statistics = immutable_options.stats;

  CompressionType compression_type = kNoCompression;

  {
    const Status s = ReadHeader(file_reader.get(), column_family_id, statistics,
                                &compression_type);
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
      std::move(file_reader), file_size, compression_type,
      immutable_options.clock, statistics));

  return Status::OK();
}

Status DeltaLogFileReader::OpenFile(
    const ImmutableOptions& immutable_options, const FileOptions& file_opts,
    HistogramImpl* deltaLog_file_read_hist, uint64_t deltaLog_file_number,
    const std::shared_ptr<IOTracer>& io_tracer, uint64_t* file_size,
    std::unique_ptr<RandomAccessFileReader>* file_reader) {
  assert(file_size);
  assert(file_reader);

  const auto& cf_paths = immutable_options.cf_paths;
  assert(!cf_paths.empty());

  const std::string deltaLog_file_path =
      DeltaLogFileName(cf_paths.front().path, deltaLog_file_number);

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

  if (*file_size < DeltaLogLogHeader::kSize + DeltaLogLogFooter::kSize) {
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
                                      Statistics* statistics,
                                      CompressionType* compression_type) {
  assert(file_reader);
  assert(compression_type);

  Slice header_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  {
    TEST_SYNC_POINT("DeltaLogFileReader::ReadHeader:ReadFromFile");

    constexpr uint64_t read_offset = 0;
    constexpr size_t read_size = DeltaLogLogHeader::kSize;

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

  DeltaLogLogHeader header;

  {
    const Status s = header.DecodeFrom(header_slice);
    if (!s.ok()) {
      return s;
    }
  }

  constexpr ExpirationRange no_expiration_range;

  if (header.has_ttl || header.expiration_range != no_expiration_range) {
    return Status::Corruption("Unexpected TTL deltaLog file");
  }

  if (header.column_family_id != column_family_id) {
    return Status::Corruption("Column family ID mismatch");
  }

  *compression_type = header.compression;

  return Status::OK();
}

Status DeltaLogFileReader::ReadFooter(const RandomAccessFileReader* file_reader,
                                      uint64_t file_size,
                                      Statistics* statistics) {
  assert(file_size >= DeltaLogLogHeader::kSize + DeltaLogLogFooter::kSize);
  assert(file_reader);

  Slice footer_slice;
  Buffer buf;
  AlignedBuf aligned_buf;

  {
    TEST_SYNC_POINT("DeltaLogFileReader::ReadFooter:ReadFromFile");

    const uint64_t read_offset = file_size - DeltaLogLogFooter::kSize;
    constexpr size_t read_size = DeltaLogLogFooter::kSize;

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

  DeltaLogLogFooter footer;

  {
    const Status s = footer.DecodeFrom(footer_slice);
    if (!s.ok()) {
      return s;
    }
  }

  constexpr ExpirationRange no_expiration_range;

  if (footer.expiration_range != no_expiration_range) {
    return Status::Corruption("Unexpected TTL deltaLog file");
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
    CompressionType compression_type, SystemClock* clock,
    Statistics* statistics)
    : file_reader_(std::move(file_reader)),
      file_size_(file_size),
      compression_type_(compression_type),
      clock_(clock),
      statistics_(statistics) {
  assert(file_reader_);
}

DeltaLogFileReader::~DeltaLogFileReader() = default;

Status DeltaLogFileReader::GetDeltaLog(
    const ReadOptions& read_options, const Slice& user_key, uint64_t offset,
    uint64_t value_size, CompressionType compression_type,
    FilePrefetchBuffer* prefetch_buffer, MemoryAllocator* allocator,
    std::unique_ptr<DeltaLogContents>* result, uint64_t* bytes_read) const {
  assert(result);

  const uint64_t key_size = user_key.size();

  if (!IsValidDeltaLogOffset(offset, key_size, value_size, file_size_)) {
    return Status::Corruption("Invalid deltaLog offset");
  }

  if (compression_type != compression_type_) {
    return Status::Corruption(
        "Compression type mismatch when reading deltaLog");
  }

  // Note: if verify_checksum is set, we read the entire deltaLog record to be
  // able to perform the verification; otherwise, we just read the deltaLog
  // itself. Since the offset in DeltaLogIndex actually points to the deltaLog
  // value, we need to make an adjustment in the former case.
  const uint64_t adjustment =
      read_options.verify_checksums
          ? DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(key_size)
          : 0;
  assert(offset >= adjustment);

  const uint64_t record_offset = offset - adjustment;
  const uint64_t record_size = value_size + adjustment;

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

  const Slice value_slice(record_slice.data() + adjustment, value_size);

  {
    const Status s = UncompressDeltaLogIfNeeded(
        value_slice, compression_type, allocator, clock_, statistics_, result);
    if (!s.ok()) {
      return s;
    }
  }

  if (bytes_read) {
    *bytes_read = record_size;
  }

  return Status::OK();
}

void DeltaLogFileReader::MultiGetDeltaLog(
    const ReadOptions& read_options, MemoryAllocator* allocator,
    autovector<std::pair<DeltaLogReadRequest*,
                         std::unique_ptr<DeltaLogContents>>>& deltaLog_reqs,
    uint64_t* bytes_read) const {
  const size_t num_deltaLogs = deltaLog_reqs.size();
  assert(num_deltaLogs > 0);
  assert(num_deltaLogs <= MultiGetContext::MAX_BATCH_SIZE);

#ifndef NDEBUG
  for (size_t i = 0; i < num_deltaLogs - 1; ++i) {
    assert(deltaLog_reqs[i].first->offset <=
           deltaLog_reqs[i + 1].first->offset);
  }
#endif  // !NDEBUG

  std::vector<FSReadRequest> read_reqs;
  autovector<uint64_t> adjustments;
  uint64_t total_len = 0;
  read_reqs.reserve(num_deltaLogs);
  for (size_t i = 0; i < num_deltaLogs; ++i) {
    DeltaLogReadRequest* const req = deltaLog_reqs[i].first;
    assert(req);
    assert(req->user_key);
    assert(req->status);

    const size_t key_size = req->user_key->size();
    const uint64_t offset = req->offset;
    const uint64_t value_size = req->len;

    if (!IsValidDeltaLogOffset(offset, key_size, value_size, file_size_)) {
      *req->status = Status::Corruption("Invalid deltaLog offset");
      continue;
    }
    if (req->compression != compression_type_) {
      *req->status = Status::Corruption(
          "Compression type mismatch when reading a deltaLog");
      continue;
    }

    const uint64_t adjustment =
        read_options.verify_checksums
            ? DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(key_size)
            : 0;
    assert(req->offset >= adjustment);
    adjustments.push_back(adjustment);

    FSReadRequest read_req = {};
    read_req.offset = req->offset - adjustment;
    read_req.len = req->len + adjustment;
    read_reqs.emplace_back(read_req);
    total_len += read_req.len;
  }

  RecordTick(statistics_, DELTALOG_DB_DELTALOG_FILE_BYTES_READ, total_len);

  Buffer buf;
  AlignedBuf aligned_buf;

  Status s;
  bool direct_io = file_reader_->use_direct_io();
  if (direct_io) {
    for (size_t i = 0; i < read_reqs.size(); ++i) {
      read_reqs[i].scratch = nullptr;
    }
  } else {
    buf.reset(new char[total_len]);
    std::ptrdiff_t pos = 0;
    for (size_t i = 0; i < read_reqs.size(); ++i) {
      read_reqs[i].scratch = buf.get() + pos;
      pos += read_reqs[i].len;
    }
  }
  TEST_SYNC_POINT("DeltaLogFileReader::MultiGetDeltaLog:ReadFromFile");
  PERF_COUNTER_ADD(deltaLog_read_count, num_deltaLogs);
  PERF_COUNTER_ADD(deltaLog_read_byte, total_len);
  s = file_reader_->MultiRead(IOOptions(), read_reqs.data(), read_reqs.size(),
                              direct_io ? &aligned_buf : nullptr,
                              read_options.rate_limiter_priority);
  if (!s.ok()) {
    for (auto& req : read_reqs) {
      req.status.PermitUncheckedError();
    }
    for (auto& deltaLog_req : deltaLog_reqs) {
      DeltaLogReadRequest* const req = deltaLog_req.first;
      assert(req);
      assert(req->status);

      if (!req->status->IsCorruption()) {
        // Avoid overwriting corruption status.
        *req->status = s;
      }
    }
    return;
  }

  assert(s.ok());

  uint64_t total_bytes = 0;
  for (size_t i = 0, j = 0; i < num_deltaLogs; ++i) {
    DeltaLogReadRequest* const req = deltaLog_reqs[i].first;
    assert(req);
    assert(req->user_key);
    assert(req->status);

    if (!req->status->ok()) {
      continue;
    }

    assert(j < read_reqs.size());
    auto& read_req = read_reqs[j++];
    const auto& record_slice = read_req.result;
    if (read_req.status.ok() && record_slice.size() != read_req.len) {
      read_req.status =
          IOStatus::Corruption("Failed to read data from deltaLog file");
    }

    *req->status = read_req.status;
    if (!req->status->ok()) {
      continue;
    }

    // Verify checksums if enabled
    if (read_options.verify_checksums) {
      *req->status = VerifyDeltaLog(record_slice, *req->user_key, req->len);
      if (!req->status->ok()) {
        continue;
      }
    }

    // Uncompress deltaLog if needed
    Slice value_slice(record_slice.data() + adjustments[i], req->len);
    *req->status = UncompressDeltaLogIfNeeded(value_slice, compression_type_,
                                              allocator, clock_, statistics_,
                                              &deltaLog_reqs[i].second);
    if (req->status->ok()) {
      total_bytes += record_slice.size();
    }
  }

  if (bytes_read) {
    *bytes_read = total_bytes;
  }
}

Status DeltaLogFileReader::VerifyDeltaLog(const Slice& record_slice,
                                          const Slice& user_key,
                                          uint64_t value_size) {
  PERF_TIMER_GUARD(deltaLog_checksum_time);

  DeltaLogLogRecord record;

  const Slice header_slice(record_slice.data(), DeltaLogLogRecord::kHeaderSize);

  {
    const Status s = record.DecodeHeaderFrom(header_slice);
    if (!s.ok()) {
      return s;
    }
  }

  if (record.key_size != user_key.size()) {
    return Status::Corruption("Key size mismatch when reading deltaLog");
  }

  if (record.value_size != value_size) {
    return Status::Corruption("Value size mismatch when reading deltaLog");
  }

  record.key = Slice(record_slice.data() + DeltaLogLogRecord::kHeaderSize,
                     record.key_size);
  if (record.key != user_key) {
    return Status::Corruption("Key mismatch when reading deltaLog");
  }

  record.value = Slice(record.key.data() + record.key_size, value_size);

  {
    TEST_SYNC_POINT_CALLBACK(
        "DeltaLogFileReader::VerifyDeltaLog:CheckDeltaLogCRC", &record);

    const Status s = record.CheckDeltaLogCRC();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status DeltaLogFileReader::UncompressDeltaLogIfNeeded(
    const Slice& value_slice, CompressionType compression_type,
    MemoryAllocator* allocator, SystemClock* clock, Statistics* statistics,
    std::unique_ptr<DeltaLogContents>* result) {
  assert(result);

  if (compression_type == kNoCompression) {
    CacheAllocationPtr allocation =
        AllocateBlock(value_slice.size(), allocator);
    memcpy(allocation.get(), value_slice.data(), value_slice.size());

    *result =
        DeltaLogContents::Create(std::move(allocation), value_slice.size());

    return Status::OK();
  }

  UncompressionContext context(compression_type);
  UncompressionInfo info(context, UncompressionDict::GetEmptyDict(),
                         compression_type);

  size_t uncompressed_size = 0;
  constexpr uint32_t compression_format_version = 2;

  CacheAllocationPtr output;

  {
    PERF_TIMER_GUARD(deltaLog_decompress_time);
    StopWatch stop_watch(clock, statistics, DELTALOG_DB_DECOMPRESSION_MICROS);
    output = UncompressData(info, value_slice.data(), value_slice.size(),
                            &uncompressed_size, compression_format_version,
                            allocator);
  }

  TEST_SYNC_POINT_CALLBACK(
      "DeltaLogFileReader::UncompressDeltaLogIfNeeded:TamperWithResult",
      &output);

  if (!output) {
    return Status::Corruption("Unable to uncompress deltaLog");
  }

  *result = DeltaLogContents::Create(std::move(output), uncompressed_size);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
