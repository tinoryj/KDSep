
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE
#include "utilities/delta_db/delta_file.h"

#include <stdio.h>

#include <algorithm>
#include <cinttypes>
#include <memory>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "file/filename.h"
#include "file/readahead_raf.h"
#include "logging/logging.h"
#include "utilities/delta_db/delta_db_impl.h"

namespace ROCKSDB_NAMESPACE {

namespace delta_db {

DeltaFile::DeltaFile(const DeltaDBImpl* p, const std::string& bdir, uint64_t fn,
                     Logger* info_log)
    : parent_(p), path_to_dir_(bdir), file_number_(fn), info_log_(info_log) {}

DeltaFile::DeltaFile(const DeltaDBImpl* p, const std::string& bdir, uint64_t fn,
                     Logger* info_log, uint32_t column_family_id,
                     CompressionType compression, bool has_ttl,
                     const ExpirationRange& expiration_range)
    : parent_(p),
      path_to_dir_(bdir),
      file_number_(fn),
      info_log_(info_log),
      column_family_id_(column_family_id),
      compression_(compression),
      has_ttl_(has_ttl),
      expiration_range_(expiration_range),
      header_(column_family_id, compression, has_ttl, expiration_range),
      header_valid_(true) {}

DeltaFile::~DeltaFile() {
  if (obsolete_) {
    std::string pn(PathName());
    Status s = Env::Default()->DeleteFile(PathName());
    if (!s.ok()) {
      // ROCKS_LOG_INFO(db_options_.info_log,
      // "File could not be deleted %s", pn.c_str());
    }
  }
}

uint32_t DeltaFile::GetColumnFamilyId() const { return column_family_id_; }

std::string DeltaFile::PathName() const {
  return DeltaFileName(path_to_dir_, file_number_);
}

std::string DeltaFile::DumpState() const {
  char str[1000];
  snprintf(str, sizeof(str),
           "path: %s fn: %" PRIu64 " delta_count: %" PRIu64
           " file_size: %" PRIu64
           " closed: %d obsolete: %d expiration_range: (%" PRIu64 ", %" PRIu64
           "), writer: %d reader: %d",
           path_to_dir_.c_str(), file_number_, delta_count_.load(),
           file_size_.load(), closed_.load(), obsolete_.load(),
           expiration_range_.first, expiration_range_.second, (!!log_writer_),
           (!!ra_file_reader_));
  return str;
}

void DeltaFile::MarkObsolete(SequenceNumber sequence) {
  assert(Immutable());
  obsolete_sequence_ = sequence;
  obsolete_.store(true);
}

Status DeltaFile::WriteFooterAndCloseLocked(SequenceNumber sequence) {
  DeltaLogFooter footer;
  footer.delta_count = delta_count_;
  if (HasTTL()) {
    footer.expiration_range = expiration_range_;
  }

  // this will close the file and reset the Writable File Pointer.
  Status s = log_writer_->AppendFooter(footer, /* checksum_method */ nullptr,
                                       /* checksum_value */ nullptr);
  if (s.ok()) {
    closed_ = true;
    immutable_sequence_ = sequence;
    file_size_ += DeltaLogFooter::kSize;
  }
  // delete the sequential writer
  log_writer_.reset();
  return s;
}

Status DeltaFile::ReadFooter(DeltaLogFooter* bf) {
  if (file_size_ < (DeltaLogHeader::kSize + DeltaLogFooter::kSize)) {
    return Status::IOError("File does not have footer", PathName());
  }

  uint64_t footer_offset = file_size_ - DeltaLogFooter::kSize;
  // assume that ra_file_reader_ is valid before we enter this
  assert(ra_file_reader_);

  Slice result;
  std::string buf;
  AlignedBuf aligned_buf;
  Status s;
  // TODO: rate limit reading footers from delta files.
  if (ra_file_reader_->use_direct_io()) {
    s = ra_file_reader_->Read(IOOptions(), footer_offset, DeltaLogFooter::kSize,
                              &result, nullptr, &aligned_buf,
                              Env::IO_TOTAL /* rate_limiter_priority */);
  } else {
    buf.reserve(DeltaLogFooter::kSize + 10);
    s = ra_file_reader_->Read(IOOptions(), footer_offset, DeltaLogFooter::kSize,
                              &result, &buf[0], nullptr,
                              Env::IO_TOTAL /* rate_limiter_priority */);
  }
  if (!s.ok()) return s;
  if (result.size() != DeltaLogFooter::kSize) {
    // should not happen
    return Status::IOError("EOF reached before footer");
  }

  s = bf->DecodeFrom(result);
  return s;
}

Status DeltaFile::SetFromFooterLocked(const DeltaLogFooter& footer) {
  delta_count_ = footer.delta_count;
  expiration_range_ = footer.expiration_range;
  closed_ = true;
  return Status::OK();
}

Status DeltaFile::Fsync() {
  Status s;
  if (log_writer_.get()) {
    s = log_writer_->Sync();
  }
  return s;
}

void DeltaFile::CloseRandomAccessLocked() {
  ra_file_reader_.reset();
  last_access_ = -1;
}

Status DeltaFile::GetReader(Env* env, const FileOptions& file_options,
                            std::shared_ptr<RandomAccessFileReader>* reader,
                            bool* fresh_open) {
  assert(reader != nullptr);
  assert(fresh_open != nullptr);
  *fresh_open = false;
  int64_t current_time = 0;
  if (env->GetCurrentTime(&current_time).ok()) {
    last_access_.store(current_time);
  }
  Status s;

  {
    ReadLock lockbfile_r(&mutex_);
    if (ra_file_reader_) {
      *reader = ra_file_reader_;
      return s;
    }
  }

  WriteLock lockbfile_w(&mutex_);
  // Double check.
  if (ra_file_reader_) {
    *reader = ra_file_reader_;
    return s;
  }

  std::unique_ptr<FSRandomAccessFile> rfile;
  s = env->GetFileSystem()->NewRandomAccessFile(PathName(), file_options,
                                                &rfile, nullptr);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to open delta file for random-read: %s status: '%s'"
                    " exists: '%s'",
                    PathName().c_str(), s.ToString().c_str(),
                    env->FileExists(PathName()).ToString().c_str());
    return s;
  }

  ra_file_reader_ =
      std::make_shared<RandomAccessFileReader>(std::move(rfile), PathName());
  *reader = ra_file_reader_;
  *fresh_open = true;
  return s;
}

Status DeltaFile::ReadMetadata(const std::shared_ptr<FileSystem>& fs,
                               const FileOptions& file_options) {
  assert(Immutable());
  // Get file size.
  uint64_t file_size = 0;
  Status s =
      fs->GetFileSize(PathName(), file_options.io_options, &file_size, nullptr);
  if (s.ok()) {
    file_size_ = file_size;
  } else {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to get size of delta file %" PRIu64 ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  if (file_size < DeltaLogHeader::kSize) {
    ROCKS_LOG_ERROR(info_log_,
                    "Incomplete delta file delta file %" PRIu64
                    ", size: %" PRIu64,
                    file_number_, file_size);
    return Status::Corruption("Incomplete delta file header.");
  }

  // Create file reader.
  std::unique_ptr<RandomAccessFileReader> file_reader;
  s = RandomAccessFileReader::Create(fs, PathName(), file_options, &file_reader,
                                     nullptr);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to open delta file %" PRIu64 ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }

  // Read file header.
  std::string header_buf;
  AlignedBuf aligned_buf;
  Slice header_slice;
  // TODO: rate limit reading headers from delta files.
  if (file_reader->use_direct_io()) {
    s = file_reader->Read(IOOptions(), 0, DeltaLogHeader::kSize, &header_slice,
                          nullptr, &aligned_buf,
                          Env::IO_TOTAL /* rate_limiter_priority */);
  } else {
    header_buf.reserve(DeltaLogHeader::kSize);
    s = file_reader->Read(IOOptions(), 0, DeltaLogHeader::kSize, &header_slice,
                          &header_buf[0], nullptr,
                          Env::IO_TOTAL /* rate_limiter_priority */);
  }
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to read header of delta file %" PRIu64
                    ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  DeltaLogHeader header;
  s = header.DecodeFrom(header_slice);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to decode header of delta file %" PRIu64
                    ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  column_family_id_ = header.column_family_id;
  compression_ = header.compression;
  has_ttl_ = header.has_ttl;
  if (has_ttl_) {
    expiration_range_ = header.expiration_range;
  }
  header_valid_ = true;

  // Read file footer.
  if (file_size_ < DeltaLogHeader::kSize + DeltaLogFooter::kSize) {
    // OK not to have footer.
    assert(!footer_valid_);
    return Status::OK();
  }
  std::string footer_buf;
  Slice footer_slice;
  // TODO: rate limit reading footers from delta files.
  if (file_reader->use_direct_io()) {
    s = file_reader->Read(IOOptions(), file_size - DeltaLogFooter::kSize,
                          DeltaLogFooter::kSize, &footer_slice, nullptr,
                          &aligned_buf,
                          Env::IO_TOTAL /* rate_limiter_priority */);
  } else {
    footer_buf.reserve(DeltaLogFooter::kSize);
    s = file_reader->Read(IOOptions(), file_size - DeltaLogFooter::kSize,
                          DeltaLogFooter::kSize, &footer_slice, &footer_buf[0],
                          nullptr, Env::IO_TOTAL /* rate_limiter_priority */);
  }
  if (!s.ok()) {
    ROCKS_LOG_ERROR(info_log_,
                    "Failed to read footer of delta file %" PRIu64
                    ", status: %s",
                    file_number_, s.ToString().c_str());
    return s;
  }
  DeltaLogFooter footer;
  s = footer.DecodeFrom(footer_slice);
  if (!s.ok()) {
    // OK not to have footer.
    assert(!footer_valid_);
    return Status::OK();
  }
  delta_count_ = footer.delta_count;
  if (has_ttl_) {
    assert(header.expiration_range.first <= footer.expiration_range.first);
    assert(header.expiration_range.second >= footer.expiration_range.second);
    expiration_range_ = footer.expiration_range;
  }
  footer_valid_ = true;
  return Status::OK();
}

}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
