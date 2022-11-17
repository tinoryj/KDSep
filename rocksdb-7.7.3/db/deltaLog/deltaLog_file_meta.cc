//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_file_meta.h"

#include <ostream>
#include <sstream>

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
uint64_t DeltaLogFileMetaData::GetDeltaLogFileSize() {
  return DeltaLogHeader::kSize_ + total_deltaLog_bytes_ +
         DeltaLogFooter::kSize_;
}

DeltaLogFileMetaData::DeltaLogFileMetaData(uint64_t deltaLog_file_id,
                                           uint64_t total_deltaLog_count,
                                           uint64_t total_deltaLog_bytes)
    : deltaLog_file_id_(deltaLog_file_id),
      total_deltaLog_count_(total_deltaLog_bytes),
      total_deltaLog_bytes_(total_deltaLog_bytes) {}

bool DeltaLogFileMetaData::UpdateTotalDeltaLogCount(uint64_t deltaLogCount) {
  total_deltaLog_count_ += deltaLogCount;
  return true;
}
bool DeltaLogFileMetaData::UpdateTotalDeltaLogBytes(uint64_t deltaLogSize) {
  total_deltaLog_bytes_ += deltaLogSize;
  return true;
}

std::ostream& operator<<(std::ostream& os, DeltaLogFileMetaData& shared_meta) {
  os << "deltaLog_file_id: " << shared_meta.GetDeltaLogFileID()
     << " total_deltaLog_count: " << shared_meta.GetTotalDeltaLogCount()
     << " total_deltaLog_bytes: " << shared_meta.GetTotalDeltaLogBytes();

  return os;
}

Status DeltaLogIndex::GenerateFullFileHashFromKey(Slice slice) {
  u_char murmurHashResultBuffer[16];
  MurmurHash3_x64_128((void*)slice.data(), slice.size(), 0,
                      murmurHashResultBuffer);
  memcpy(&filePrefixHashFull_, murmurHashResultBuffer, sizeof(uint64_t));
  if (filePrefixHashFull_ == 0) {
    return Status::OK();
  } else {
    return Status::Aborted();
  }
}

DeltaLogFileMetaData* DeltaLogFileManager::GetDeltaLogFileMetaData(
    uint64_t userKeyFullHash) {
  std::string ans;
  while (userKeyFullHash != 0) {
    ans += (userKeyFullHash & 1) + '0';
    userKeyFullHash >>= 1;
  }
  reverse(ans.begin(), ans.end());
  std::string prefixStr;
  if (ans.size() > initialTrieBitNumber_) {
    prefixStr = ans.substr(1, initialTrieBitNumber_);
  }
  if (deltaLogFileMetaDataTrie_.exist(prefixStr)) {
    Trie<DeltaLogFileMetaData>::iterator it =
        deltaLogFileMetaDataTrie_.find(prefixStr);
    return &(*it);
  } else {
    return nullptr;
  }
}

DeltaLogFileMetaData* DeltaLogFileManager::GetDeltaLogFileMetaDataByFileID(
    uint64_t deltaLogFileID) {
  if (deltaLogIDToPrefixMap_.find(deltaLogFileID) ==
      deltaLogIDToPrefixMap_.end()) {
    return nullptr;
  } else {
    std::string prefixStr = deltaLogIDToPrefixMap_.at(deltaLogFileID);
    if (deltaLogFileMetaDataTrie_.exist(prefixStr)) {
      Trie<DeltaLogFileMetaData>::iterator it =
          deltaLogFileMetaDataTrie_.find(prefixStr);
      return &(*it);
    } else {
      return nullptr;
    }
  }
}

bool DeltaLogFileManager::AddDeltaLogFileMetaData(uint64_t userKeyFullHash,
                                                  uint64_t deltaLogFileID) {
  std::string ans;
  while (userKeyFullHash != 0) {
    ans += (userKeyFullHash & 1) + '0';
    userKeyFullHash >>= 1;
  }
  reverse(ans.begin(), ans.end());
  std::string prefixStr;
  if (ans.size() > initialTrieBitNumber_) {
    prefixStr = ans.substr(1, initialTrieBitNumber_);
  }
  DeltaLogFileMetaData newDeltaLogFileMetaData(deltaLogFileID, 0, 0);
  deltaLogFileMetaDataTrie_.insert(prefixStr, newDeltaLogFileMetaData);
  deltaLogIDToPrefixMap_.insert(make_pair(deltaLogFileID, prefixStr));
  return true;
}

bool DeltaLogFileManager::UpdateDeltaLogFileMetaData(uint64_t deltaLogFileID,
                                                     uint64_t deltaLogNumber,
                                                     uint64_t deltaLogSize) {
  totalDeltaLogFileSize_ += deltaLogNumber;
  totalDeltaLogFileNumber_ += deltaLogSize;
  Trie<DeltaLogFileMetaData>::iterator it =
      deltaLogFileMetaDataTrie_.find(deltaLogIDToPrefixMap_.at(deltaLogFileID));
  bool updateSizeStatus = it->UpdateTotalDeltaLogBytes(deltaLogSize);
  bool updateCountStatus = it->UpdateTotalDeltaLogCount(deltaLogNumber);
  if (updateSizeStatus && updateCountStatus) {
    if (it->GetTotalDeltaLogBytes() >= deltaLogGCTriggerSize_) {
      deltaLogMetaDataSelectedForGC_.insert(&(*it));
    }
    if (totalDeltaLogFileSize_ >= deltaLogGlobalGCTriggerSize_) {
      shouldDeltaLogPerformGlobalGC_ = true;
    }
    return true;
  } else {
    return false;
  }
}

uint64_t DeltaLogFileManager::ProcessDeltaLogGC() {}

}  // namespace ROCKSDB_NAMESPACE
