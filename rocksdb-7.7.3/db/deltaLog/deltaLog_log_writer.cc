//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_log_writer.h"

#include <cstdint>
#include <string>

#include "db/deltaLog/deltaLog_log_format.h"
#include "file/writable_file_writer.h"
#include "monitoring/statistics.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

DeltaLogLogWriter::DeltaLogLogWriter(std::unique_ptr<WritableFileWriter>&& dest,
                                     SystemClock* clock, Statistics* statistics,
                                     uint64_t log_number, bool use_fs,
                                     bool do_flush, uint64_t boffset)
    : dest_(std::move(dest)),
      clock_(clock),
      statistics_(statistics),
      log_number_(log_number),
      block_offset_(boffset),
      use_fsync_(use_fs),
      do_flush_(do_flush),
      last_elem_type_(kEtNone) {}

DeltaLogLogWriter::~DeltaLogLogWriter() = default;

Status DeltaLogLogWriter::Sync() {
  TEST_SYNC_POINT("DeltaLogLogWriter::Sync");

  StopWatch sync_sw(clock_, statistics_, DELTALOG_DB_DELTALOG_FILE_SYNC_MICROS);
  Status s = dest_->Sync(use_fsync_);
  RecordTick(statistics_, DELTALOG_DB_DELTALOG_FILE_SYNCED);
  return s;
}

Status DeltaLogLogWriter::WriteHeader(DeltaLogHeader& header) {
  assert(block_offset_ == 0);
  assert(last_elem_type_ == kEtNone);
  std::string str;
  header.EncodeTo(&str);

  Status s = dest_->Append(Slice(str));
  if (s.ok()) {
    block_offset_ += str.size();
    if (do_flush_) {
      s = dest_->Flush();
    }
  }
  last_elem_type_ = kEtFileHdr;
  RecordTick(statistics_, DELTALOG_DB_DELTALOG_FILE_BYTES_WRITTEN,
             DeltaLogHeader::kSize_);
  return s;
}

Status DeltaLogLogWriter::AppendFooter(DeltaLogFooter& footer) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == kEtFileHdr || last_elem_type_ == kEtRecord);

  std::string str;
  footer.EncodeTo(&str);

  Status s;
  if (dest_->seen_error()) {
    s.PermitUncheckedError();
    return Status::IOError("Seen Error. Skip closing.");
  } else {
    s = dest_->Append(Slice(str));
    if (s.ok()) {
      block_offset_ += str.size();

      s = Sync();

      if (s.ok()) {
        s = dest_->Close();
      }
    }

    dest_.reset();
  }

  last_elem_type_ = kEtFileFooter;
  RecordTick(statistics_, DELTALOG_DB_DELTALOG_FILE_BYTES_WRITTEN,
             DeltaLogFooter::kSize_);
  return s;
}

Status DeltaLogLogWriter::AddRecord(const Slice& key, const Slice& val,
                                    bool is_anchor) {
  assert(block_offset_ != 0);
  assert(last_elem_type_ == kEtFileHdr || last_elem_type_ == kEtRecord);

  std::string buf;
  ConstructDeltaLogHeader(&buf, key, val, is_anchor);

  Status s = EmitPhysicalRecord(buf, key, val);
  return s;
}

void DeltaLogLogWriter::ConstructDeltaLogHeader(std::string* buf,
                                                const Slice& key,
                                                const Slice& val,
                                                bool is_anchor) {
  DeltaLogRecord record;
  record.key_ = key;
  record.value_ = val;
  record.is_anchor_ = is_anchor;
  record.EncodeHeaderTo(buf);
}

Status DeltaLogLogWriter::EmitPhysicalRecord(const std::string& headerbuf,
                                             const Slice& key,
                                             const Slice& val) {
  StopWatch write_sw(clock_, statistics_,
                     DELTALOG_DB_DELTALOG_FILE_WRITE_MICROS);
  Status s = dest_->Append(Slice(headerbuf));
  if (s.ok()) {
    s = dest_->Append(key);
  }
  if (s.ok()) {
    s = dest_->Append(val);
  }
  if (do_flush_ && s.ok()) {
    s = dest_->Flush();
  }

  block_offset_ += DeltaLogRecord::kHeaderSize_ + key.size() + val.size();
  last_elem_type_ = kEtRecord;
  RecordTick(statistics_, DELTALOG_DB_DELTALOG_FILE_BYTES_WRITTEN,
             DeltaLogRecord::kHeaderSize_ + key.size() + val.size());
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
