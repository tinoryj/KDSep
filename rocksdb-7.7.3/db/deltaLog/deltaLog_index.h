//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <bitset>
#include <sstream>
#include <string>

#include "murmurHash3.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class DeltaLogIndex {
 public:
  DeltaLogIndex() {}

  DeltaLogIndex(const DeltaLogIndex&) = default;
  DeltaLogIndex& operator=(const DeltaLogIndex&) = default;

  uint64_t getFilePrefixHash() const { return filePrefixHash_; }

  uint64_t getFileID() const { return final_file_id_; }

  uint64_t getFileSize() const { return final_file_size_; }

  Status GenerateFullFileHashFromKey(Slice slice) {
    u_char murmurHashResultBuffer[16];
    MurmurHash3_x64_128((void*)slice.data(), slice.size(), 0,
                        murmurHashResultBuffer);
    memcpy(&filePrefixHash_, murmurHashResultBuffer, sizeof(uint64_t));
    if (filePrefixHash_ == 0) {
      return Status::OK();
    } else {
      return Status::Aborted();
    }
  }

  std::string DebugString(bool output_hex) const {
    std::ostringstream oss;

    oss << "[deltaLog ref] target file hash (full):" << filePrefixHash_;
    return oss.str();
  }

  Status GenerateDeltaLogIndex(int currentPrefixTreeBitNumber) {
    std::bitset<currentPrefixTreeBitNumber> final_file_id_bitset;
    for (int i = 0; i < currentPrefixTreeBitNumber; i++) {
      final_file_id_bitset[i] = ((filePrefixHash_ & (1 << i)) >> i);
    }
    final_file_id_ = final_file_id_bitset.to_ullong();
    return Status::OK();
  }

 private:
  uint64_t filePrefixHash_ = 0;
  uint64_t final_file_id_ = 0;
  uint64_t final_file_size_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
