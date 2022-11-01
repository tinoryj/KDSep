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
// or checksum method and value. There is supposed to be one object of this
// class per deltaLog file (shared across all versions that include the deltaLog
// file in question); hence, the type is neither copyable nor movable. A
// deltaLog file can be marked obsolete when the corresponding
// SharedDeltaLogFileMetaData object is destroyed.

class SharedDeltaLogFileMetaData {
 public:
  static std::shared_ptr<SharedDeltaLogFileMetaData> Create(
      uint64_t deltaLog_file_number, uint64_t total_deltaLog_count,
      uint64_t total_deltaLog_bytes, std::string checksum_method,
      std::string checksum_value) {
    return std::shared_ptr<SharedDeltaLogFileMetaData>(
        new SharedDeltaLogFileMetaData(
            deltaLog_file_number, total_deltaLog_count, total_deltaLog_bytes,
            std::move(checksum_method), std::move(checksum_value)));
  }

  template <typename Deleter>
  static std::shared_ptr<SharedDeltaLogFileMetaData> Create(
      uint64_t deltaLog_file_number, uint64_t total_deltaLog_count,
      uint64_t total_deltaLog_bytes, std::string checksum_method,
      std::string checksum_value, Deleter deleter) {
    return std::shared_ptr<SharedDeltaLogFileMetaData>(
        new SharedDeltaLogFileMetaData(
            deltaLog_file_number, total_deltaLog_count, total_deltaLog_bytes,
            std::move(checksum_method), std::move(checksum_value)),
        deleter);
  }

  SharedDeltaLogFileMetaData(const SharedDeltaLogFileMetaData&) = delete;
  SharedDeltaLogFileMetaData& operator=(const SharedDeltaLogFileMetaData&) =
      delete;

  SharedDeltaLogFileMetaData(SharedDeltaLogFileMetaData&&) = delete;
  SharedDeltaLogFileMetaData& operator=(SharedDeltaLogFileMetaData&&) = delete;

  uint64_t GetDeltaLogFileSize() const;
  uint64_t GetDeltaLogFileNumber() const { return deltaLog_file_number_; }
  uint64_t GetTotalDeltaLogCount() const { return total_deltaLog_count_; }
  uint64_t GetTotalDeltaLogBytes() const { return total_deltaLog_bytes_; }
  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }

  std::string DebugString() const;

 private:
  SharedDeltaLogFileMetaData(uint64_t deltaLog_file_number,
                             uint64_t total_deltaLog_count,
                             uint64_t total_deltaLog_bytes,
                             std::string checksum_method,
                             std::string checksum_value)
      : deltaLog_file_number_(deltaLog_file_number),
        total_deltaLog_count_(total_deltaLog_count),
        total_deltaLog_bytes_(total_deltaLog_bytes),
        checksum_method_(std::move(checksum_method)),
        checksum_value_(std::move(checksum_value)) {
    assert(checksum_method_.empty() == checksum_value_.empty());
  }

  uint64_t deltaLog_file_number_;
  uint64_t total_deltaLog_count_;
  uint64_t total_deltaLog_bytes_;
  std::string checksum_method_;
  std::string checksum_value_;
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
  using LinkedSsts = std::unordered_set<uint64_t>;

  static std::shared_ptr<DeltaLogFileMetaData> Create(
      std::shared_ptr<SharedDeltaLogFileMetaData> shared_meta,
      LinkedSsts linked_ssts, uint64_t garbage_deltaLog_count,
      uint64_t garbage_deltaLog_bytes) {
    return std::shared_ptr<DeltaLogFileMetaData>(new DeltaLogFileMetaData(
        std::move(shared_meta), std::move(linked_ssts), garbage_deltaLog_count,
        garbage_deltaLog_bytes));
  }

  DeltaLogFileMetaData(const DeltaLogFileMetaData&) = delete;
  DeltaLogFileMetaData& operator=(const DeltaLogFileMetaData&) = delete;

  DeltaLogFileMetaData(DeltaLogFileMetaData&&) = delete;
  DeltaLogFileMetaData& operator=(DeltaLogFileMetaData&&) = delete;

  const std::shared_ptr<SharedDeltaLogFileMetaData>& GetSharedMeta() const {
    return shared_meta_;
  }

  uint64_t GetDeltaLogFileSize() const {
    assert(shared_meta_);
    return shared_meta_->GetDeltaLogFileSize();
  }

  uint64_t GetDeltaLogFileNumber() const {
    assert(shared_meta_);
    return shared_meta_->GetDeltaLogFileNumber();
  }
  uint64_t GetTotalDeltaLogCount() const {
    assert(shared_meta_);
    return shared_meta_->GetTotalDeltaLogCount();
  }
  uint64_t GetTotalDeltaLogBytes() const {
    assert(shared_meta_);
    return shared_meta_->GetTotalDeltaLogBytes();
  }
  const std::string& GetChecksumMethod() const {
    assert(shared_meta_);
    return shared_meta_->GetChecksumMethod();
  }
  const std::string& GetChecksumValue() const {
    assert(shared_meta_);
    return shared_meta_->GetChecksumValue();
  }

  const LinkedSsts& GetLinkedSsts() const { return linked_ssts_; }

  uint64_t GetGarbageDeltaLogCount() const { return garbage_deltaLog_count_; }
  uint64_t GetGarbageDeltaLogBytes() const { return garbage_deltaLog_bytes_; }

  std::string DebugString() const;

 private:
  DeltaLogFileMetaData(std::shared_ptr<SharedDeltaLogFileMetaData> shared_meta,
                       LinkedSsts linked_ssts, uint64_t garbage_deltaLog_count,
                       uint64_t garbage_deltaLog_bytes)
      : shared_meta_(std::move(shared_meta)),
        linked_ssts_(std::move(linked_ssts)),
        garbage_deltaLog_count_(garbage_deltaLog_count),
        garbage_deltaLog_bytes_(garbage_deltaLog_bytes) {
    assert(shared_meta_);
    assert(garbage_deltaLog_count_ <= shared_meta_->GetTotalDeltaLogCount());
    assert(garbage_deltaLog_bytes_ <= shared_meta_->GetTotalDeltaLogBytes());
  }

  std::shared_ptr<SharedDeltaLogFileMetaData> shared_meta_;
  LinkedSsts linked_ssts_;
  uint64_t garbage_deltaLog_count_;
  uint64_t garbage_deltaLog_bytes_;
};

std::ostream& operator<<(std::ostream& os, const DeltaLogFileMetaData& meta);

}  // namespace ROCKSDB_NAMESPACE
