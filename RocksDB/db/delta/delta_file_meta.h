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

// SharedDeltaFileMetaData represents the immutable part of delta files'
// metadata, like the delta file number, total number and size of deltas, or
// checksum method and value. There is supposed to be one object of this class
// per delta file (shared across all versions that include the delta file in
// question); hence, the type is neither copyable nor movable. A delta file can
// be marked obsolete when the corresponding SharedDeltaFileMetaData object is
// destroyed.

class SharedDeltaFileMetaData {
 public:
  static std::shared_ptr<SharedDeltaFileMetaData> Create(
      uint64_t delta_file_number, uint64_t total_delta_count,
      uint64_t total_delta_bytes, std::string checksum_method,
      std::string checksum_value) {
    return std::shared_ptr<SharedDeltaFileMetaData>(new SharedDeltaFileMetaData(
        delta_file_number, total_delta_count, total_delta_bytes,
        std::move(checksum_method), std::move(checksum_value)));
  }

  template <typename Deleter>
  static std::shared_ptr<SharedDeltaFileMetaData> Create(
      uint64_t delta_file_number, uint64_t total_delta_count,
      uint64_t total_delta_bytes, std::string checksum_method,
      std::string checksum_value, Deleter deleter) {
    return std::shared_ptr<SharedDeltaFileMetaData>(
        new SharedDeltaFileMetaData(
            delta_file_number, total_delta_count, total_delta_bytes,
            std::move(checksum_method), std::move(checksum_value)),
        deleter);
  }

  SharedDeltaFileMetaData(const SharedDeltaFileMetaData&) = delete;
  SharedDeltaFileMetaData& operator=(const SharedDeltaFileMetaData&) = delete;

  SharedDeltaFileMetaData(SharedDeltaFileMetaData&&) = delete;
  SharedDeltaFileMetaData& operator=(SharedDeltaFileMetaData&&) = delete;

  uint64_t GetDeltaFileSize() const;
  uint64_t GetDeltaFileNumber() const { return delta_file_number_; }
  uint64_t GetTotalDeltaCount() const { return total_delta_count_; }
  uint64_t GetTotalDeltaBytes() const { return total_delta_bytes_; }
  const std::string& GetChecksumMethod() const { return checksum_method_; }
  const std::string& GetChecksumValue() const { return checksum_value_; }

  std::string DebugString() const;

 private:
  SharedDeltaFileMetaData(uint64_t delta_file_number,
                          uint64_t total_delta_count,
                          uint64_t total_delta_bytes,
                          std::string checksum_method,
                          std::string checksum_value)
      : delta_file_number_(delta_file_number),
        total_delta_count_(total_delta_count),
        total_delta_bytes_(total_delta_bytes),
        checksum_method_(std::move(checksum_method)),
        checksum_value_(std::move(checksum_value)) {
    assert(checksum_method_.empty() == checksum_value_.empty());
  }

  uint64_t delta_file_number_;
  uint64_t total_delta_count_;
  uint64_t total_delta_bytes_;
  std::string checksum_method_;
  std::string checksum_value_;
};

std::ostream& operator<<(std::ostream& os,
                         const SharedDeltaFileMetaData& shared_meta);

// DeltaFileMetaData contains the part of the metadata for delta files that can
// vary across versions, like the amount of garbage in the delta file. In
// addition, DeltaFileMetaData objects point to and share the ownership of the
// SharedDeltaFileMetaData object for the corresponding delta file. Similarly to
// SharedDeltaFileMetaData, DeltaFileMetaData are not copyable or movable. They
// are meant to be jointly owned by the versions in which the delta file has the
// same (immutable *and* mutable) state.

class DeltaFileMetaData {
 public:
  using LinkedSsts = std::unordered_set<uint64_t>;

  static std::shared_ptr<DeltaFileMetaData> Create(
      std::shared_ptr<SharedDeltaFileMetaData> shared_meta,
      LinkedSsts linked_ssts, uint64_t garbage_delta_count,
      uint64_t garbage_delta_bytes) {
    return std::shared_ptr<DeltaFileMetaData>(
        new DeltaFileMetaData(std::move(shared_meta), std::move(linked_ssts),
                              garbage_delta_count, garbage_delta_bytes));
  }

  DeltaFileMetaData(const DeltaFileMetaData&) = delete;
  DeltaFileMetaData& operator=(const DeltaFileMetaData&) = delete;

  DeltaFileMetaData(DeltaFileMetaData&&) = delete;
  DeltaFileMetaData& operator=(DeltaFileMetaData&&) = delete;

  const std::shared_ptr<SharedDeltaFileMetaData>& GetSharedMeta() const {
    return shared_meta_;
  }

  uint64_t GetDeltaFileSize() const {
    assert(shared_meta_);
    return shared_meta_->GetDeltaFileSize();
  }

  uint64_t GetDeltaFileNumber() const {
    assert(shared_meta_);
    return shared_meta_->GetDeltaFileNumber();
  }
  uint64_t GetTotalDeltaCount() const {
    assert(shared_meta_);
    return shared_meta_->GetTotalDeltaCount();
  }
  uint64_t GetTotalDeltaBytes() const {
    assert(shared_meta_);
    return shared_meta_->GetTotalDeltaBytes();
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

  uint64_t GetGarbageDeltaCount() const { return garbage_delta_count_; }
  uint64_t GetGarbageDeltaBytes() const { return garbage_delta_bytes_; }

  std::string DebugString() const;

 private:
  DeltaFileMetaData(std::shared_ptr<SharedDeltaFileMetaData> shared_meta,
                    LinkedSsts linked_ssts, uint64_t garbage_delta_count,
                    uint64_t garbage_delta_bytes)
      : shared_meta_(std::move(shared_meta)),
        linked_ssts_(std::move(linked_ssts)),
        garbage_delta_count_(garbage_delta_count),
        garbage_delta_bytes_(garbage_delta_bytes) {
    assert(shared_meta_);
    assert(garbage_delta_count_ <= shared_meta_->GetTotalDeltaCount());
    assert(garbage_delta_bytes_ <= shared_meta_->GetTotalDeltaBytes());
  }

  std::shared_ptr<SharedDeltaFileMetaData> shared_meta_;
  LinkedSsts linked_ssts_;
  uint64_t garbage_delta_count_;
  uint64_t garbage_delta_bytes_;
};

std::ostream& operator<<(std::ostream& os, const DeltaFileMetaData& meta);

}  // namespace ROCKSDB_NAMESPACE
