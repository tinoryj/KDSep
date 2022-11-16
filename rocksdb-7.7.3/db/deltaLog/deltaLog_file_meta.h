//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_set>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// SharedDeltaLogFileMetaData represents the immutable part of deltaLog files'
// metadata, like the deltaLog file number, total number and size of deltaLogs,
// and value. There is supposed to be one object of this class per deltaLog file
// (shared across all versions that include the deltaLog file in question);
// hence, the type is neither copyable nor movable. A deltaLog file can be
// marked obsolete when the corresponding SharedDeltaLogFileMetaData object is
// destroyed.

class SharedDeltaLogFileMetaData {
 public:
  static std::shared_ptr<SharedDeltaLogFileMetaData> Create(
      uint64_t deltaLog_file_id, uint64_t total_deltaLog_count,
      uint64_t total_deltaLog_bytes) {
    return std::shared_ptr<SharedDeltaLogFileMetaData>(
        new SharedDeltaLogFileMetaData(deltaLog_file_id, total_deltaLog_count,
                                       total_deltaLog_bytes));
  }

  template <typename Deleter>
  static std::shared_ptr<SharedDeltaLogFileMetaData> Create(
      uint64_t deltaLog_file_id, uint64_t total_deltaLog_count,
      uint64_t total_deltaLog_bytes, Deleter deleter) {
    return std::shared_ptr<SharedDeltaLogFileMetaData>(
        new SharedDeltaLogFileMetaData(deltaLog_file_id, total_deltaLog_count,
                                       total_deltaLog_bytes),
        deleter);
  }

  SharedDeltaLogFileMetaData(const SharedDeltaLogFileMetaData&) = delete;
  SharedDeltaLogFileMetaData& operator=(const SharedDeltaLogFileMetaData&) =
      delete;

  SharedDeltaLogFileMetaData(SharedDeltaLogFileMetaData&&) = delete;
  SharedDeltaLogFileMetaData& operator=(SharedDeltaLogFileMetaData&&) = delete;

  uint64_t GetDeltaLogFileSize() const;
  uint64_t GetDeltaLogFileID() const { return deltaLog_file_id_; }
  uint64_t GetTotalDeltaLogCount() const { return total_deltaLog_count_; }
  uint64_t GetTotalDeltaLogBytes() const { return total_deltaLog_bytes_; }

 private:
  SharedDeltaLogFileMetaData(uint64_t deltaLog_file_id,
                             uint64_t total_deltaLog_count,
                             uint64_t total_deltaLog_bytes)
      : deltaLog_file_id_(deltaLog_file_id),
        total_deltaLog_count_(total_deltaLog_count),
        total_deltaLog_bytes_(total_deltaLog_bytes) {}

  uint64_t deltaLog_file_id_;
  uint64_t total_deltaLog_count_;
  uint64_t total_deltaLog_bytes_;
};

std::ostream& operator<<(std::ostream& os,
                         const SharedDeltaLogFileMetaData& shared_meta);

// DeltaLogFileMetaData contains the part of the metadata for deltaLog files
// that can vary across versions, like the amount of garbage in the deltaLog
// file. In addition, DeltaLogFileMetaData objects point to and share the
// ownership of the SharedDeltaLogFileMetaData object for the corresponding
// deltaLog file. Similarly to SharedDeltaLogFileMetaData, DeltaLogFileMetaData
// are not copyable or movable. They are meant to be jointly owned by the
// versions in which the deltaLog file has the same (immutable *and* mutable)
// state.

class DeltaLogFileMetaData {
 public:
  static std::shared_ptr<DeltaLogFileMetaData> Create(
      std::shared_ptr<SharedDeltaLogFileMetaData> shared_meta,
      uint64_t garbage_deltaLog_count, uint64_t garbage_deltaLog_bytes) {
    return std::shared_ptr<DeltaLogFileMetaData>(
        new DeltaLogFileMetaData(std::move(shared_meta), garbage_deltaLog_count,
                                 garbage_deltaLog_bytes));
  }

  DeltaLogFileMetaData(const DeltaLogFileMetaData&) = delete;
  DeltaLogFileMetaData& operator=(const DeltaLogFileMetaData&) = delete;

  DeltaLogFileMetaData(DeltaLogFileMetaData&&) = delete;
  DeltaLogFileMetaData& operator=(DeltaLogFileMetaData&&) = delete;

  const std::shared_ptr<SharedDeltaLogFileMetaData>& GetSharedMeta() const {
    return shared_deltaLog_meta_;
  }

  uint64_t GetDeltaLogFileSize() const {
    assert(shared_deltaLog_meta_);
    return shared_deltaLog_meta_->GetDeltaLogFileSize();
  }

  uint64_t GetDeltaLogFileID() const {
    assert(shared_deltaLog_meta_);
    return shared_deltaLog_meta_->GetDeltaLogFileID();
  }
  uint64_t GetTotalDeltaLogCount() const {
    assert(shared_deltaLog_meta_);
    return shared_deltaLog_meta_->GetTotalDeltaLogCount();
  }
  uint64_t GetTotalDeltaLogBytes() const {
    assert(shared_deltaLog_meta_);
    return shared_deltaLog_meta_->GetTotalDeltaLogBytes();
  }

  uint64_t GetGarbageDeltaLogCount() const { return garbage_deltaLog_count_; }
  uint64_t GetGarbageDeltaLogBytes() const { return garbage_deltaLog_bytes_; }

  std::string DebugString() const;

 private:
  DeltaLogFileMetaData(std::shared_ptr<SharedDeltaLogFileMetaData> shared_meta,
                       uint64_t garbage_deltaLog_count,
                       uint64_t garbage_deltaLog_bytes)
      : shared_deltaLog_meta_(std::move(shared_meta)),
        garbage_deltaLog_count_(garbage_deltaLog_count),
        garbage_deltaLog_bytes_(garbage_deltaLog_bytes) {
    assert(shared_deltaLog_meta_);
    assert(garbage_deltaLog_count_ <=
           shared_deltaLog_meta_->GetTotalDeltaLogCount());
    assert(garbage_deltaLog_bytes_ <=
           shared_deltaLog_meta_->GetTotalDeltaLogBytes());
  }

  std::shared_ptr<SharedDeltaLogFileMetaData> shared_deltaLog_meta_;
  uint64_t garbage_deltaLog_count_;
  uint64_t garbage_deltaLog_bytes_;
};

std::ostream& operator<<(std::ostream& os, const DeltaLogFileMetaData& meta);

}  // namespace ROCKSDB_NAMESPACE
