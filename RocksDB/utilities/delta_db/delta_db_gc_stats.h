//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

#ifndef ROCKSDB_LITE

namespace ROCKSDB_NAMESPACE {

namespace delta_db {

/**
 * Statistics related to a single garbage collection pass (i.e. a single
 * (sub)compaction).
 */
class DeltaDBGarbageCollectionStats {
 public:
  uint64_t AllDeltas() const { return all_deltas_; }
  uint64_t AllBytes() const { return all_bytes_; }
  uint64_t RelocatedDeltas() const { return relocated_deltas_; }
  uint64_t RelocatedBytes() const { return relocated_bytes_; }
  uint64_t NewFiles() const { return new_files_; }
  bool HasError() const { return error_; }

  void AddDelta(uint64_t size) {
    ++all_deltas_;
    all_bytes_ += size;
  }

  void AddRelocatedDelta(uint64_t size) {
    ++relocated_deltas_;
    relocated_bytes_ += size;
  }

  void AddNewFile() { ++new_files_; }

  void SetError() { error_ = true; }

 private:
  uint64_t all_deltas_ = 0;
  uint64_t all_bytes_ = 0;
  uint64_t relocated_deltas_ = 0;
  uint64_t relocated_bytes_ = 0;
  uint64_t new_files_ = 0;
  bool error_ = false;
};

}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
