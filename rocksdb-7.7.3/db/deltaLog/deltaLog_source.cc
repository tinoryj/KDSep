//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_source.h"

#include <cassert>
#include <string>

#include "cache/cache_reservation_manager.h"
#include "cache/charged_cache.h"
#include "db/deltaLog/deltaLog_contents.h"
#include "db/deltaLog/deltaLog_file_reader.h"
#include "db/deltaLog/deltaLog_log_format.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "table/get_context.h"
#include "table/multiget_context.h"

namespace ROCKSDB_NAMESPACE {

DeltaLogSource::DeltaLogSource(const ImmutableOptions* immutable_options,
                               const std::string& db_id,
                               const std::string& db_session_id,
                               DeltaLogFileCache* deltaLog_file_cache,
                               DeltaLogFileManager* deltaLogFileManager)
    : db_id_(db_id),
      db_session_id_(db_session_id),
      statistics_(immutable_options->statistics.get()),
      deltaLog_file_cache_(deltaLog_file_cache),
      deltaLog_cache_(immutable_options->deltaLog_cache),
      lowest_used_cache_tier_(immutable_options->lowest_used_cache_tier),
      deltaLogFileManager_(deltaLogFileManager) {
#ifndef ROCKSDB_LITE
  auto bbto =
      immutable_options->table_factory->GetOptions<BlockBasedTableOptions>();
  if (bbto && bbto->cache_usage_options.options_overrides
                      .at(CacheEntryRole::kDeltaLogCache)
                      .charged == CacheEntryRoleOptions::Decision::kEnabled) {
    deltaLog_cache_ = std::make_shared<ChargedCache>(
        immutable_options->deltaLog_cache, bbto->block_cache);
  }
#endif  // ROCKSDB_LITE
}

DeltaLogSource::~DeltaLogSource() = default;

Status DeltaLogSource::GetDeltaLogFromCache(
    const Slice& cache_key,
    CacheHandleGuard<DeltaLogContents>* cached_deltaLog) const {
  assert(deltaLog_cache_);
  assert(!cache_key.empty());
  assert(cached_deltaLog);
  assert(cached_deltaLog->IsEmpty());

  Cache::Handle* cache_handle = nullptr;
  cache_handle = GetEntryFromCache(cache_key);
  if (cache_handle != nullptr) {
    *cached_deltaLog =
        CacheHandleGuard<DeltaLogContents>(deltaLog_cache_.get(), cache_handle);

    assert(cached_deltaLog->GetValue());

    PERF_COUNTER_ADD(deltaLog_cache_hit_count, 1);
    RecordTick(statistics_, DELTALOG_DB_CACHE_HIT);
    RecordTick(statistics_, DELTALOG_DB_CACHE_BYTES_READ,
               cached_deltaLog->GetValue()->size());

    return Status::OK();
  }

  RecordTick(statistics_, DELTALOG_DB_CACHE_MISS);

  return Status::NotFound("DeltaLog not found in cache");
}

Status DeltaLogSource::PutDeltaLogIntoCache(
    const Slice& cache_key, std::unique_ptr<DeltaLogContents>* deltaLog,
    CacheHandleGuard<DeltaLogContents>* cached_deltaLog) const {
  assert(deltaLog_cache_);
  assert(!cache_key.empty());
  assert(deltaLog);
  assert(*deltaLog);
  assert(cached_deltaLog);
  assert(cached_deltaLog->IsEmpty());

  Cache::Handle* cache_handle = nullptr;
  const Status s = InsertEntryIntoCache(cache_key, deltaLog->get(),
                                        (*deltaLog)->ApproximateMemoryUsage(),
                                        &cache_handle, Cache::Priority::BOTTOM);
  if (s.ok()) {
    deltaLog->release();

    assert(cache_handle != nullptr);
    *cached_deltaLog =
        CacheHandleGuard<DeltaLogContents>(deltaLog_cache_.get(), cache_handle);

    assert(cached_deltaLog->GetValue());

    RecordTick(statistics_, DELTALOG_DB_CACHE_ADD);
    RecordTick(statistics_, DELTALOG_DB_CACHE_BYTES_WRITE,
               cached_deltaLog->GetValue()->size());

  } else {
    RecordTick(statistics_, DELTALOG_DB_CACHE_ADD_FAILURES);
  }

  return s;
}

Cache::Handle* DeltaLogSource::GetEntryFromCache(const Slice& key) const {
  Cache::Handle* cache_handle = nullptr;

  if (lowest_used_cache_tier_ == CacheTier::kNonVolatileBlockTier) {
    Cache::CreateCallback create_cb =
        [allocator = deltaLog_cache_->memory_allocator()](
            const void* buf, size_t size, void** out_obj,
            size_t* charge) -> Status {
      return DeltaLogContents::CreateCallback(AllocateBlock(size, allocator),
                                              buf, size, out_obj, charge);
    };

    cache_handle = deltaLog_cache_->Lookup(
        key, DeltaLogContents::GetCacheItemHelper(), create_cb,
        Cache::Priority::BOTTOM, true /* wait_for_cache */, statistics_);
  } else {
    cache_handle = deltaLog_cache_->Lookup(key, statistics_);
  }

  return cache_handle;
}

Status DeltaLogSource::InsertEntryIntoCache(const Slice& key,
                                            DeltaLogContents* value,
                                            size_t charge,
                                            Cache::Handle** cache_handle,
                                            Cache::Priority priority) const {
  Status s;

  Cache::CacheItemHelper* const cache_item_helper =
      DeltaLogContents::GetCacheItemHelper();
  assert(cache_item_helper);

  if (lowest_used_cache_tier_ == CacheTier::kNonVolatileBlockTier) {
    s = deltaLog_cache_->Insert(key, value, cache_item_helper, charge,
                                cache_handle, priority);
  } else {
    s = deltaLog_cache_->Insert(key, value, charge, cache_item_helper->del_cb,
                                cache_handle, priority);
  }

  return s;
}

Status DeltaLogSource::GetDeltaLog(const ReadOptions& read_options,
                                   const Slice& user_key, uint64_t file_id,
                                   autovector<Slice>& value_vec,
                                   uint64_t* bytes_read) {
  Status s;

  CacheHandleGuard<DeltaLogContents> deltaLog_cache_handle;

  // First, try to get the deltaLog from the cache
  //
  // If deltaLog cache is enabled, we'll try to read from it.
  if (deltaLog_cache_) {
    s = GetDeltaLogFromCache(user_key, &deltaLog_cache_handle);
    if (s.ok()) {
      Slice rawValue = deltaLog_cache_handle.GetValue()->data();
      uint64_t record_size = rawValue.size();
      if (bytes_read) {
        *bytes_read = record_size;
      }
      uint64_t readIndex = 0;
      while (true) {
        if (readIndex == rawValue.size()) {
          break;
        }
        DeltaLogRecord tempRecord;
        Slice newHeaderSlice(rawValue.data() + readIndex,
                             DeltaLogRecord::kHeaderSize_);
        tempRecord.DecodeHeaderFrom(newHeaderSlice);
        if (tempRecord.is_anchor_ == true) {
          value_vec.clear();
          readIndex += (tempRecord.key_size_ + tempRecord.value_size_ +
                        DeltaLogRecord::kHeaderSize_);
          continue;
        } else {
          readIndex += (DeltaLogRecord::kHeaderSize_ + tempRecord.key_size_);
          Slice newValueSlice(rawValue.data() + readIndex,
                              tempRecord.value_size_);
          value_vec.emplace_back(newValueSlice);
          readIndex += tempRecord.value_size_;
        }
      }
      return s;
    }
  }

  assert(deltaLog_cache_handle.IsEmpty());

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    s = Status::Incomplete("Cannot read deltaLog(s): no disk I/O allowed");
    return s;
  }

  // Can't find the deltaLog from the cache. Since I/O is allowed, read from the
  // file.
  std::unique_ptr<DeltaLogContents> deltaLog_contents;

  {
    CacheHandleGuard<DeltaLogFileReader> deltaLog_file_reader;
    s = deltaLog_file_cache_->GetDeltaLogFileReader(
        file_id, &deltaLog_file_reader,
        deltaLogFileManager_->GetDeltaLogFileMetaDataByFileID(file_id));
    if (!s.ok()) {
      return s;
    }

    assert(deltaLog_file_reader.GetValue());

    MemoryAllocator* const allocator =
        (deltaLog_cache_ && read_options.fill_cache)
            ? deltaLog_cache_->memory_allocator()
            : nullptr;

    uint64_t read_size = 0;
    s = deltaLog_file_reader.GetValue()->GetDeltaLog(
        read_options, user_key, file_id, allocator, &deltaLog_contents,
        &read_size);
    if (!s.ok()) {
      return s;
    }
    if (bytes_read) {
      *bytes_read = read_size;
    }
  }

  if (deltaLog_cache_ && read_options.fill_cache) {
    // If filling cache is allowed and a cache is configured, try to put the
    // deltaLog to the cache.

    std::unordered_map<std::string, std::vector<std::string>>
        fillCacheContentsMap;
    uint64_t readIndex = 0;
    while (true) {
      if (readIndex == deltaLog_contents->size()) {
        break;
      }
      DeltaLogRecord tempRecord;
      Slice newHeaderSlice(deltaLog_contents->data().data() + readIndex,
                           DeltaLogRecord::kHeaderSize_);
      tempRecord.DecodeHeaderFrom(newHeaderSlice);
      if (tempRecord.is_anchor_ == true) {
        readIndex += DeltaLogRecord::kHeaderSize_;
        std::string userKeyStr(deltaLog_contents->data().data() + readIndex,
                               tempRecord.key_size_);
        if (fillCacheContentsMap.find(userKeyStr) !=
            fillCacheContentsMap.end()) {
          fillCacheContentsMap.at(userKeyStr).clear();
        }
        readIndex += (tempRecord.key_size_ + tempRecord.value_size_);
        continue;
      } else {
        std::string userKeyStr(deltaLog_contents->data().data() + readIndex +
                                   DeltaLogRecord::kHeaderSize_,
                               tempRecord.key_size_);
        if (fillCacheContentsMap.find(userKeyStr) !=
            fillCacheContentsMap.end()) {
          Slice newValueSlice(deltaLog_contents->data().data() + readIndex,
                              tempRecord.value_size_);
          int currentItemSize = DeltaLogRecord::kHeaderSize_ +
                                tempRecord.key_size_ + tempRecord.value_size_;
          std::string pushBackValueStr(
              deltaLog_contents->data().data() + readIndex, currentItemSize);
          fillCacheContentsMap.at(userKeyStr).push_back(pushBackValueStr);
          readIndex += currentItemSize;
        }
      }
    }
    for (auto userKeyIterator : fillCacheContentsMap) {
      std::string newDeltaLogContentsStr;
      bool currentKeyFlag = false;
      if (memcmp(userKeyIterator.first.c_str(), user_key.data(),
                 user_key.size()) == 0) {
        for (auto currentKeyDeltaPairIterator : userKeyIterator.second) {
          newDeltaLogContentsStr.append(currentKeyDeltaPairIterator);
          // append current userkey
          DeltaLogRecord tempRecord;
          Slice tempRecordSlice(currentKeyDeltaPairIterator.c_str(),
                                DeltaLogRecord::kHeaderSize_);
          tempRecord.DecodeHeaderFrom(tempRecordSlice);
          Slice newValueSlice(currentKeyDeltaPairIterator.c_str() +
                                  DeltaLogRecord::kHeaderSize_ +
                                  tempRecord.key_size_,
                              tempRecord.value_size_);
          value_vec.emplace_back(newValueSlice);
        }
      } else {
        for (auto currentKeyDeltaPairIterator : userKeyIterator.second) {
          newDeltaLogContentsStr.append(currentKeyDeltaPairIterator);
        }
      }
      CacheAllocationPtr allocation = AllocateBlock(
          newDeltaLogContentsStr.size(), deltaLog_cache_->memory_allocator());
      memcpy(allocation.get(), newDeltaLogContentsStr.c_str(),
             newDeltaLogContentsStr.size());
      Slice newDeltaLogContentsSlice(newDeltaLogContentsStr.c_str(),
                                     newDeltaLogContentsStr.size());
      Slice newDeltaLogKeySlice(userKeyIterator.first.c_str(),
                                newDeltaLogContentsStr.size());
      std::unique_ptr<DeltaLogContents> currentKeyRelatedBuf =
          DeltaLogContents::Create(std::move(allocation),
                                   newDeltaLogContentsSlice.size());
      s = PutDeltaLogIntoCache(newDeltaLogKeySlice, &currentKeyRelatedBuf,
                               &deltaLog_cache_handle);
      if (!s.ok()) {
        std::cout << "Error insert new deltaLog contents into cache"
                  << std::endl;
        return s;
      }
    }
    return Status::OK();
  } else {
    std::vector<std::string> currentUserKeyContentsVec;
    uint64_t readIndex = 0;
    while (true) {
      if (readIndex == deltaLog_contents->size()) {
        break;
      }
      DeltaLogRecord tempRecord;
      Slice newHeaderSlice(deltaLog_contents->data().data() + readIndex,
                           DeltaLogRecord::kHeaderSize_);
      tempRecord.DecodeHeaderFrom(newHeaderSlice);
      if (tempRecord.is_anchor_ == true) {
        readIndex += DeltaLogRecord::kHeaderSize_;
        std::string userKeyStr(deltaLog_contents->data().data() + readIndex,
                               tempRecord.key_size_);
        if (memcmp(userKeyStr.c_str(), user_key.data(), user_key.size()) == 0) {
          value_vec.clear();
        }
        readIndex += (tempRecord.key_size_ + tempRecord.value_size_);
        continue;
      } else {
        std::string userKeyStr(deltaLog_contents->data().data() + readIndex,
                               tempRecord.key_size_);
        if (memcmp(userKeyStr.c_str(), user_key.data(), user_key.size()) == 0) {
          readIndex += (DeltaLogRecord::kHeaderSize_ + tempRecord.key_size_);
          Slice newValueSlice(deltaLog_contents->data().data() + readIndex,
                              tempRecord.value_size_);
          value_vec.emplace_back(newValueSlice);
          readIndex += tempRecord.value_size_;
        } else {
          readIndex += (DeltaLogRecord::kHeaderSize_ + tempRecord.key_size_ +
                        tempRecord.value_size_);
          continue;
        }
      }
    }
    return Status::OK();
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
