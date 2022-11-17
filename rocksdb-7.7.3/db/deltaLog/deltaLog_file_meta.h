//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "db/deltaLog/deltaLog_log_format.h"
#include "db/deltaLog/murmurHash3.h"
#include "db/deltaLog/trie.h"
#include "rocksdb/rocksdb_namespace.h"
namespace ROCKSDB_NAMESPACE {

// DeltaLogFileMetaData represents the immutable part of deltaLog files'
// metadata, like the deltaLog file number, total number and size of deltaLogs,
// and value. There is supposed to be one object of this class per deltaLog file
// (shared across all versions that include the deltaLog file in question);
// hence, the type is neither copyable nor movable. A deltaLog file can be
// marked obsolete when the corresponding DeltaLogFileMetaData object is
// destroyed.

class DeltaLogFileMetaData {
 public:
  DeltaLogFileMetaData(uint64_t deltaLog_file_id, uint64_t total_deltaLog_count,
                       uint64_t total_deltaLog_bytes);
  DeltaLogFileMetaData()
      : deltaLog_file_id_(0),
        total_deltaLog_count_(0),
        total_deltaLog_bytes_(0){};
  uint64_t GetDeltaLogFileSize();
  uint64_t GetDeltaLogFileID() { return deltaLog_file_id_; }
  uint64_t GetTotalDeltaLogCount() { return total_deltaLog_count_; }
  uint64_t GetTotalDeltaLogBytes() { return total_deltaLog_bytes_; }
  bool UpdateTotalDeltaLogCount(uint64_t deltaLogCount);
  bool UpdateTotalDeltaLogBytes(uint64_t deltaLogSize);

 private:
  uint64_t deltaLog_file_id_;
  uint64_t total_deltaLog_count_;
  uint64_t total_deltaLog_bytes_;
};

std::ostream& operator<<(std::ostream& os, DeltaLogFileMetaData& shared_meta);

class DeltaLogFileManager {
 public:
  DeltaLogFileManager(uint64_t initialBitNumber, uint64_t deltaLogGCTriggerSize,
                      uint64_t deltaLogGlobalGCTriggerSize)
      : initialTrieBitNumber_(initialBitNumber),
        deltaLogGCTriggerSize_(deltaLogGCTriggerSize),
        deltaLogGlobalGCTriggerSize_(deltaLogGlobalGCTriggerSize){};
  DeltaLogFileManager& operator=(const DeltaLogFileManager&) = delete;

  DeltaLogFileMetaData* GetDeltaLogFileMetaData(uint64_t userKeyFullHash);
  DeltaLogFileMetaData* GetDeltaLogFileMetaDataByFileID(
      uint64_t deltaLogFileID);
  bool AddDeltaLogFileMetaData(uint64_t userKeyFullHash,
                               uint64_t deltaLogFileID);
  bool UpdateDeltaLogFileMetaData(uint64_t deltaLogFileID,
                                  uint64_t deltaLogNumber,
                                  uint64_t deltaLogSize);
  uint64_t GetTotalDeltaLogFileSize() { return totalDeltaLogFileSize_; }
  uint64_t GetTotalDeltaLogFileNumber() { return totalDeltaLogFileNumber_; }
  uint64_t ProcessDeltaLogGC();  // return processed file number
  bool ShouldDeltaLogGlobalGC() {
    return shouldDeltaLogPerformGlobalGC_;
  };  // return processed file number

 private:
  bool shouldDeltaLogPerformGlobalGC_ = false;
  uint64_t initialTrieBitNumber_ = 8;
  uint64_t deltaLogGCTriggerSize_ = 1 * 1024 * 1024;
  uint64_t deltaLogGlobalGCTriggerSize_ = 1 * 1024 * 1024;
  Trie<DeltaLogFileMetaData>
      deltaLogFileMetaDataTrie_;  // prefix-hash to deltaLog file metadata.
  uint64_t totalDeltaLogFileSize_ = 0;
  uint64_t totalDeltaLogFileNumber_ = 0;
  std::set<DeltaLogFileMetaData*>
      deltaLogMetaDataSelectedForGC_;  // deltaLog file -> current file size;
  std::unordered_map<uint64_t, std::string>
      deltaLogIDToPrefixMap_;  // deltaLog file id -> prefix;
};

class DeltaLogIndex {
 public:
  DeltaLogIndex() {}

  DeltaLogIndex(const DeltaLogIndex&) = default;
  DeltaLogIndex& operator=(const DeltaLogIndex&) = default;

  uint64_t getDeltaLogFilePrefixHashFull() const { return filePrefixHashFull_; }

  Status GenerateFullFileHashFromKey(Slice slice);

 private:
  uint64_t filePrefixHashFull_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
