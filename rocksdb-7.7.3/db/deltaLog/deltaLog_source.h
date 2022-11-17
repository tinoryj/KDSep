//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>

#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "db/deltaLog/deltaLog_file_cache.h"
#include "db/deltaLog/deltaLog_file_meta.h"
#include "rocksdb/cache.h"
#include "rocksdb/rocksdb_namespace.h"
#include "table/block_based/cachable_entry.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

struct ImmutableOptions;
class Status;
class FilePrefetchBuffer;
class Slice;
class DeltaLogContents;

// DeltaLogSource is a class that provides universal access to deltaLogs,
// regardless of whether they are in the deltaLog cache, secondary cache, or
// (remote) storage. Depending on user settings, it always fetch deltaLogs from
// multi-tier cache and storage with minimal cost.
class DeltaLogSource {
 public:
  DeltaLogSource(const ImmutableOptions* immutable_options,
                 const std::string& db_id, const std::string& db_session_id,
                 DeltaLogFileCache* deltaLog_file_cache,
                 DeltaLogFileManager* deltaLogFileMagemer);

  DeltaLogSource(const DeltaLogSource&) = delete;
  DeltaLogSource& operator=(const DeltaLogSource&) = delete;

  ~DeltaLogSource();

  // Read a deltaLog from the underlying cache or one deltaLog file.
  //
  // If successful, returns ok and sets "*value" to the newly retrieved
  // uncompressed deltaLog. If there was an error while fetching the deltaLog,
  // sets
  // "*value" to empty and returns a non-ok status.
  //
  // Note: For consistency, whether the deltaLog is found in the cache or on
  // disk, sets "*bytes_read" to the size of on-disk (possibly compressed)
  // deltaLog record.
  Status GetDeltaLog(const ReadOptions& read_options, const Slice& user_key,
                     uint64_t file_id, autovector<Slice>& value_vec,
                     uint64_t* bytes_read);

  inline Status GetDeltaLogFileReader(
      uint64_t deltaLog_file_id,
      CacheHandleGuard<DeltaLogFileReader>* deltaLog_file_reader) {
    return deltaLog_file_cache_->GetDeltaLogFileReader(
        deltaLog_file_id, deltaLog_file_reader,
        deltaLogFileManager_->GetDeltaLogFileMetaDataByFileID(
            deltaLog_file_id));
  }

  inline Cache* GetDeltaLogCache() const { return deltaLog_cache_.get(); }

  bool TEST_DeltaLogInCache(uint64_t file_number, uint64_t file_size,
                            uint64_t offset, size_t* charge = nullptr) const;

  DeltaLogFileManager* deltaLogFileManager_;

 private:
  Status GetDeltaLogFromCache(
      const Slice& cache_key,
      CacheHandleGuard<DeltaLogContents>* cached_deltaLog) const;

  Status PutDeltaLogIntoCache(
      const Slice& cache_key, std::unique_ptr<DeltaLogContents>* deltaLog,
      CacheHandleGuard<DeltaLogContents>* cached_deltaLog) const;

  Cache::Handle* GetEntryFromCache(const Slice& key) const;

  Status InsertEntryIntoCache(const Slice& key, DeltaLogContents* value,
                              size_t charge, Cache::Handle** cache_handle,
                              Cache::Priority priority) const;

  const std::string& db_id_;
  const std::string& db_session_id_;

  Statistics* statistics_;

  // A cache to store deltaLog file reader.
  DeltaLogFileCache* deltaLog_file_cache_;

  // A cache to store uncompressed deltaLogs.
  std::shared_ptr<Cache> deltaLog_cache_;

  // The control option of how the cache tiers will be used. Currently rocksdb
  // support block/deltaLog cache (volatile tier) and secondary cache (this tier
  // isn't strictly speaking a non-volatile tier since the compressed cache in
  // this tier is in volatile memory).
  const CacheTier lowest_used_cache_tier_;
};

}  // namespace ROCKSDB_NAMESPACE
