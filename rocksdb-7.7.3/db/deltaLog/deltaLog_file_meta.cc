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

bool DeltaLogFileManager::UpdateSettingsWithDir(
    uint64_t initialBitNumber, uint64_t deltaLogGCTriggerSize,
    uint64_t deltaLogGlobalGCTriggerSize, std::string workingDir) {
  initialTrieBitNumber_ = initialBitNumber;
  deltaLogGCTriggerSize_ = deltaLogGCTriggerSize;
  deltaLogGlobalGCTriggerSize_ = deltaLogGlobalGCTriggerSize;
  workingDir_ = workingDir;
  std::cerr << "DeltaLog file manager settings updated" << std::endl;
};

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

bool DeltaLogFileManager::RetriveDeltaLogFileMetaDataList(
    std::string workingDir) {
  std::ifstream deltaLogFileManifestPointerStream;
  deltaLogFileManifestPointerStream.open(
      workingDir + "deltaLogFileManifest.pointer", std::ios::in);
  std::string currentPointerStr;
  if (deltaLogFileManifestPointerStream.is_open()) {
    getline(deltaLogFileManifestPointerStream, currentPointerStr);
    uint64_t currentPointerInt = stoull(currentPointerStr);
  } else {
    if (CreateDeltaLogFileMetaDataListIfNotExist(workingDir)) {
      return true;
    } else {
      return false;
    }
  }
  std::ifstream deltaLogFileManifestStream;
  deltaLogFileManifestStream.open(
      workingDir + "deltaLogFileManifest." + currentPointerStr, std::ios::in);
  std::string currentLineStr;
  if (deltaLogFileManifestStream.is_open()) {
    while (getline(deltaLogFileManifestStream, currentLineStr)) {
      std::string prefixHashStr = currentLineStr;
      getline(deltaLogFileManifestStream, currentLineStr);
      uint64_t deltaLogFileID = stoull(currentLineStr);
      getline(deltaLogFileManifestStream, currentLineStr);
      uint64_t totalDeltaLogCount = stoull(currentLineStr);
      getline(deltaLogFileManifestStream, currentLineStr);
      uint64_t totalDeltaLogBytes = stoull(currentLineStr);
      DeltaLogFileMetaData newDeltaLogFileMetaData(
          deltaLogFileID, totalDeltaLogCount, totalDeltaLogBytes);
      deltaLogFileMetaDataTrie_.insert(prefixHashStr, newDeltaLogFileMetaData);
      deltaLogIDToPrefixMap_.insert(make_pair(deltaLogFileID, prefixHashStr));
    }
  } else {
    std::cerr
        << "[DeltaLogFileManager]-[Error]: Could not found deltaLog manifest"
        << std::endl;
    return false;
  }
  return true;
}

bool DeltaLogFileManager::UpdateDeltaLogFileMetaDataList(
    std::string workingDir) {
  std::ifstream deltaLogFileManifestPointerStream;
  deltaLogFileManifestPointerStream.open(
      workingDir + "deltaLogFileManifest.pointer", std::ios::in);
  uint64_t currentPointerInt = 0;
  if (deltaLogFileManifestPointerStream.is_open()) {
    deltaLogFileManifestPointerStream >> currentPointerInt;
    currentPointerInt++;
  } else {
    std::cerr << "[DeltaLogFileManager]-[Error]: Could not found deltaLog "
                 "manifest pointer"
              << std::endl;
    return false;
  }
  deltaLogFileManifestPointerStream.close();
  std::ofstream deltaLogFileManifestStream;
  deltaLogFileManifestStream.open(
      workingDir + "deltaLogFileManifest." + std::to_string(currentPointerInt),
      std::ios::out);
  if (deltaLogFileMetaDataTrie_.size() != 0) {
    for (Trie<DeltaLogFileMetaData>::iterator it =
             deltaLogFileMetaDataTrie_.begin();
         it != deltaLogFileMetaDataTrie_.end(); it++) {
      deltaLogFileManifestStream
          << deltaLogIDToPrefixMap_.at(it->GetDeltaLogFileID()) << std::endl;
      deltaLogFileManifestStream << it->GetDeltaLogFileID() << std::endl;
      deltaLogFileManifestStream << it->GetTotalDeltaLogCount() << std::endl;
      deltaLogFileManifestStream << it->GetTotalDeltaLogBytes() << std::endl;
    }
    deltaLogFileManifestStream.flush();
    deltaLogFileManifestStream.close();
    // Update manifest pointer
    std::ofstream deltaLogFileManifestPointerUpdateStream;
    deltaLogFileManifestPointerUpdateStream.open(
        workingDir + "deltaLogFileManifest.pointer", std::ios::out);
    if (deltaLogFileManifestPointerUpdateStream.is_open()) {
      deltaLogFileManifestPointerUpdateStream << currentPointerInt;
      deltaLogFileManifestPointerUpdateStream.flush();
      deltaLogFileManifestPointerUpdateStream.close();
      return true;
    } else {
      std::cerr
          << "[DeltaLogFileManager]-[Error]: Could not update manifest pointer"
          << std::endl;
      return false;
    }
  } else {
    return true;
  }
}

bool DeltaLogFileManager::CreateDeltaLogFileMetaDataListIfNotExist(
    std::string workingDir) {
  std::ofstream deltaLogFileManifestPointerStream;
  deltaLogFileManifestPointerStream.open(
      workingDir + "deltaLogFileManifest.pointer", std::ios::out);
  uint64_t currentPointerInt = 0;
  deltaLogFileManifestPointerStream << currentPointerInt << std::endl;
  deltaLogFileManifestPointerStream.flush();
  deltaLogFileManifestPointerStream.close();
  if (deltaLogFileMetaDataTrie_.size() != 0) {
    std::ofstream deltaLogFileManifestStream;
    deltaLogFileManifestStream.open(workingDir + "deltaLogFileManifest." +
                                        std::to_string(currentPointerInt),
                                    std::ios::out);
    for (Trie<DeltaLogFileMetaData>::iterator it =
             deltaLogFileMetaDataTrie_.begin();
         it != deltaLogFileMetaDataTrie_.end(); it++) {
      deltaLogFileManifestStream
          << deltaLogIDToPrefixMap_.at(it->GetDeltaLogFileID()) << std::endl;
      deltaLogFileManifestStream << it->GetDeltaLogFileID() << std::endl;
      deltaLogFileManifestStream << it->GetTotalDeltaLogCount() << std::endl;
      deltaLogFileManifestStream << it->GetTotalDeltaLogBytes() << std::endl;
    }
    deltaLogFileManifestStream.flush();
    deltaLogFileManifestStream.close();
    return true;
  } else {
    return true;
  }
}

bool DeltaLogFileManager::RetriveDeltaLogFileMetaDataList() {
  assert(workingDir_);
  std::ifstream deltaLogFileManifestPointerStream;
  deltaLogFileManifestPointerStream.open(
      workingDir_ + "deltaLogFileManifest.pointer", std::ios::in);
  std::string currentPointerStr;
  if (deltaLogFileManifestPointerStream.is_open()) {
    getline(deltaLogFileManifestPointerStream, currentPointerStr);
    uint64_t currentPointerInt = stoull(currentPointerStr);
  } else {
    if (CreateDeltaLogFileMetaDataListIfNotExist()) {
      return true;
    } else {
      return false;
    }
  }
  std::ifstream deltaLogFileManifestStream;
  deltaLogFileManifestStream.open(
      workingDir_ + "deltaLogFileManifest." + currentPointerStr, std::ios::in);
  std::string currentLineStr;
  if (deltaLogFileManifestStream.is_open()) {
    while (getline(deltaLogFileManifestStream, currentLineStr)) {
      std::string prefixHashStr = currentLineStr;
      getline(deltaLogFileManifestStream, currentLineStr);
      uint64_t deltaLogFileID = stoull(currentLineStr);
      getline(deltaLogFileManifestStream, currentLineStr);
      uint64_t totalDeltaLogCount = stoull(currentLineStr);
      getline(deltaLogFileManifestStream, currentLineStr);
      uint64_t totalDeltaLogBytes = stoull(currentLineStr);
      DeltaLogFileMetaData newDeltaLogFileMetaData(
          deltaLogFileID, totalDeltaLogCount, totalDeltaLogBytes);
      deltaLogFileMetaDataTrie_.insert(prefixHashStr, newDeltaLogFileMetaData);
      deltaLogIDToPrefixMap_.insert(make_pair(deltaLogFileID, prefixHashStr));
    }
  } else {
    std::cerr
        << "[DeltaLogFileManager]-[Error]: Could not found deltaLog manifest"
        << std::endl;
    return false;
  }
  return true;
}

bool DeltaLogFileManager::UpdateDeltaLogFileMetaDataList() {
  assert(workingDir_);
  std::ifstream deltaLogFileManifestPointerStream;
  deltaLogFileManifestPointerStream.open(
      workingDir_ + "deltaLogFileManifest.pointer", std::ios::in);
  uint64_t currentPointerInt = 0;
  if (deltaLogFileManifestPointerStream.is_open()) {
    deltaLogFileManifestPointerStream >> currentPointerInt;
    currentPointerInt++;
  } else {
    std::cerr << "[DeltaLogFileManager]-[Error]: Could not found deltaLog "
                 "manifest pointer"
              << std::endl;
    return false;
  }
  deltaLogFileManifestPointerStream.close();
  std::ofstream deltaLogFileManifestStream;
  deltaLogFileManifestStream.open(
      workingDir_ + "deltaLogFileManifest." + std::to_string(currentPointerInt),
      std::ios::out);
  if (deltaLogFileMetaDataTrie_.size() != 0) {
    for (Trie<DeltaLogFileMetaData>::iterator it =
             deltaLogFileMetaDataTrie_.begin();
         it != deltaLogFileMetaDataTrie_.end(); it++) {
      deltaLogFileManifestStream
          << deltaLogIDToPrefixMap_.at(it->GetDeltaLogFileID()) << std::endl;
      deltaLogFileManifestStream << it->GetDeltaLogFileID() << std::endl;
      deltaLogFileManifestStream << it->GetTotalDeltaLogCount() << std::endl;
      deltaLogFileManifestStream << it->GetTotalDeltaLogBytes() << std::endl;
    }
    deltaLogFileManifestStream.flush();
    deltaLogFileManifestStream.close();
    // Update manifest pointer
    std::ofstream deltaLogFileManifestPointerUpdateStream;
    deltaLogFileManifestPointerUpdateStream.open(
        workingDir_ + "deltaLogFileManifest.pointer", std::ios::out);
    if (deltaLogFileManifestPointerUpdateStream.is_open()) {
      deltaLogFileManifestPointerUpdateStream << currentPointerInt;
      deltaLogFileManifestPointerUpdateStream.flush();
      deltaLogFileManifestPointerUpdateStream.close();
      return true;
    } else {
      std::cerr
          << "[DeltaLogFileManager]-[Error]: Could not update manifest pointer"
          << std::endl;
      return false;
    }
  } else {
    return true;
  }
}

bool DeltaLogFileManager::CreateDeltaLogFileMetaDataListIfNotExist() {
  assert(workingDir_);
  std::ofstream deltaLogFileManifestPointerStream;
  deltaLogFileManifestPointerStream.open(
      workingDir_ + "deltaLogFileManifest.pointer", std::ios::out);
  uint64_t currentPointerInt = 0;
  deltaLogFileManifestPointerStream << currentPointerInt << std::endl;
  deltaLogFileManifestPointerStream.flush();
  deltaLogFileManifestPointerStream.close();
  if (deltaLogFileMetaDataTrie_.size() != 0) {
    std::ofstream deltaLogFileManifestStream;
    deltaLogFileManifestStream.open(workingDir_ + "deltaLogFileManifest." +
                                        std::to_string(currentPointerInt),
                                    std::ios::out);
    for (Trie<DeltaLogFileMetaData>::iterator it =
             deltaLogFileMetaDataTrie_.begin();
         it != deltaLogFileMetaDataTrie_.end(); it++) {
      deltaLogFileManifestStream
          << deltaLogIDToPrefixMap_.at(it->GetDeltaLogFileID()) << std::endl;
      deltaLogFileManifestStream << it->GetDeltaLogFileID() << std::endl;
      deltaLogFileManifestStream << it->GetTotalDeltaLogCount() << std::endl;
      deltaLogFileManifestStream << it->GetTotalDeltaLogBytes() << std::endl;
    }
    deltaLogFileManifestStream.flush();
    deltaLogFileManifestStream.close();
    return true;
  } else {
    return true;
  }
}

uint64_t DeltaLogFileManager::ProcessDeltaLogGC() {}

}  // namespace ROCKSDB_NAMESPACE
