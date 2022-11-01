//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "db/deltaLog/deltaLog_log_sequential_reader.h"
#include "file/random_access_file_reader.h"
#include "monitoring/statistics.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

DeltaLogLogSequentialReader::DeltaLogLogSequentialReader(
    std::unique_ptr<RandomAccessFileReader>&& file_reader, SystemClock* clock,
    Statistics* statistics)
    : file_(std::move(file_reader)),
      clock_(clock),
      statistics_(statistics),
      next_byte_(0) {}

DeltaLogLogSequentialReader::~DeltaLogLogSequentialReader() = default;

Status DeltaLogLogSequentialReader::ReadSlice(uint64_t size, Slice* slice,
                                              char* buf) {
  assert(slice);
  assert(file_);

  StopWatch read_sw(clock_, statistics_, DELTALOG_DB_DELTALOG_FILE_READ_MICROS);
  // TODO: rate limit `DeltaLogLogSequentialReader` reads (it appears unused?)
  Status s =
      file_->Read(IOOptions(), next_byte_, static_cast<size_t>(size), slice,
                  buf, nullptr, Env::IO_TOTAL /* rate_limiter_priority */);
  next_byte_ += size;
  if (!s.ok()) {
    return s;
  }
  RecordTick(statistics_, DELTALOG_DB_DELTALOG_FILE_BYTES_READ, slice->size());
  if (slice->size() != size) {
    return Status::Corruption("EOF reached while reading record");
  }
  return s;
}

Status DeltaLogLogSequentialReader::ReadHeader(DeltaLogLogHeader* header) {
  assert(header);
  assert(next_byte_ == 0);

  static_assert(DeltaLogLogHeader::kSize <= sizeof(header_buf_),
                "Buffer is smaller than DeltaLogLogHeader::kSize");

  Status s = ReadSlice(DeltaLogLogHeader::kSize, &buffer_, header_buf_);
  if (!s.ok()) {
    return s;
  }

  if (buffer_.size() != DeltaLogLogHeader::kSize) {
    return Status::Corruption("EOF reached before file header");
  }

  return header->DecodeFrom(buffer_);
}

Status DeltaLogLogSequentialReader::ReadRecord(DeltaLogLogRecord* record,
                                               ReadLevel level,
                                               uint64_t* deltaLog_offset) {
  assert(record);
  static_assert(DeltaLogLogRecord::kHeaderSize <= sizeof(header_buf_),
                "Buffer is smaller than DeltaLogLogRecord::kHeaderSize");

  Status s = ReadSlice(DeltaLogLogRecord::kHeaderSize, &buffer_, header_buf_);
  if (!s.ok()) {
    return s;
  }
  if (buffer_.size() != DeltaLogLogRecord::kHeaderSize) {
    return Status::Corruption("EOF reached before record header");
  }

  s = record->DecodeHeaderFrom(buffer_);
  if (!s.ok()) {
    return s;
  }

  uint64_t kb_size = record->key_size + record->value_size;
  if (deltaLog_offset != nullptr) {
    *deltaLog_offset = next_byte_ + record->key_size;
  }

  switch (level) {
    case kReadHeader:
      next_byte_ += kb_size;
      break;

    case kReadHeaderKey:
      record->key_buf.reset(new char[record->key_size]);
      s = ReadSlice(record->key_size, &record->key, record->key_buf.get());
      next_byte_ += record->value_size;
      break;

    case kReadHeaderKeyDeltaLog:
      record->key_buf.reset(new char[record->key_size]);
      s = ReadSlice(record->key_size, &record->key, record->key_buf.get());
      if (s.ok()) {
        record->value_buf.reset(new char[record->value_size]);
        s = ReadSlice(record->value_size, &record->value,
                      record->value_buf.get());
      }
      if (s.ok()) {
        s = record->CheckDeltaLogCRC();
      }
      break;
  }
  return s;
}

Status DeltaLogLogSequentialReader::ReadFooter(DeltaLogLogFooter* footer) {
  assert(footer);
  static_assert(DeltaLogLogFooter::kSize <= sizeof(header_buf_),
                "Buffer is smaller than DeltaLogLogFooter::kSize");

  Status s = ReadSlice(DeltaLogLogFooter::kSize, &buffer_, header_buf_);
  if (!s.ok()) {
    return s;
  }

  if (buffer_.size() != DeltaLogLogFooter::kSize) {
    return Status::Corruption("EOF reached before file footer");
  }

  return footer->DecodeFrom(buffer_);
}

}  // namespace ROCKSDB_NAMESPACE
