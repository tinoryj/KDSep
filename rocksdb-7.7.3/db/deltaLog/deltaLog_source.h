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
#include "db/deltaLog/deltaLog_read_request.h"
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
                 DeltaLogFileCache* deltaLog_file_cache);

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
                     uint64_t file_number, uint64_t offset, uint64_t file_size,
                     uint64_t value_size, CompressionType compression_type,
                     FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                     uint64_t* bytes_read);

  // Read multiple deltaLogs from the underlying cache or deltaLog file(s).
  //
  // If successful, returns ok and sets "result" in the elements of
  // "deltaLog_reqs" to the newly retrieved uncompressed deltaLogs. If there was
  // an error while fetching one of deltaLogs, sets its "result" to empty and
  // sets its corresponding "status" to a non-ok status.
  //
  // Note:
  //  - The main difference between this function and
  //  MultiGetDeltaLogFromOneFile is
  //    that this function can read multiple deltaLogs from multiple deltaLog
  //    files.
  //
  //  - For consistency, whether the deltaLog is found in the cache or on disk,
  //  sets
  //  "*bytes_read" to the total size of on-disk (possibly compressed) deltaLog
  //  records.
  void MultiGetDeltaLog(const ReadOptions& read_options,
                        autovector<DeltaLogFileReadRequests>& deltaLog_reqs,
                        uint64_t* bytes_read);

  // Read multiple deltaLogs from the underlying cache or one deltaLog file.
  //
  // If successful, returns ok and sets "result" in the elements of
  // "deltaLog_reqs" to the newly retrieved uncompressed deltaLogs. If there was
  // an error while fetching one of deltaLogs, sets its "result" to empty and
  // sets its corresponding "status" to a non-ok status.
  //
  // Note:
  //  - The main difference between this function and MultiGetDeltaLog is that
  //  this function is only used for the case where the demanded deltaLogs are
  //  stored in one deltaLog file. MultiGetDeltaLog will call this function
  //  multiple times if the demanded deltaLogs are stored in multiple deltaLog
  //  files.
  //
  //  - For consistency, whether the deltaLog is found in the cache or on disk,
  //  sets
  //  "*bytes_read" to the total size of on-disk (possibly compressed) deltaLog
  //  records.
  void MultiGetDeltaLogFromOneFile(
      const ReadOptions& read_options, uint64_t file_number, uint64_t file_size,
      autovector<DeltaLogReadRequest>& deltaLog_reqs, uint64_t* bytes_read);

  inline Status GetDeltaLogFileReader(
      uint64_t deltaLog_file_number,
      CacheHandleGuard<DeltaLogFileReader>* deltaLog_file_reader) {
    return deltaLog_file_cache_->GetDeltaLogFileReader(deltaLog_file_number,
                                                       deltaLog_file_reader);
  }

  inline Cache* GetDeltaLogCache() const { return deltaLog_cache_.get(); }

  bool TEST_DeltaLogInCache(uint64_t file_number, uint64_t file_size,
                            uint64_t offset, size_t* charge = nullptr) const;

 private:
  Status GetDeltaLogFromCache(
      const Slice& cache_key,
      CacheHandleGuard<DeltaLogContents>* cached_deltaLog) const;

  Status PutDeltaLogIntoCache(
      const Slice& cache_key, std::unique_ptr<DeltaLogContents>* deltaLog,
      CacheHandleGuard<DeltaLogContents>* cached_deltaLog) const;

  static void PinCachedDeltaLog(
      CacheHandleGuard<DeltaLogContents>* cached_deltaLog,
      PinnableSlice* value);

  static void PinOwnedDeltaLog(
      std::unique_ptr<DeltaLogContents>* owned_deltaLog, PinnableSlice* value);

  Cache::Handle* GetEntryFromCache(const Slice& key) const;

  Status InsertEntryIntoCache(const Slice& key, DeltaLogContents* value,
                              size_t charge, Cache::Handle** cache_handle,
                              Cache::Priority priority) const;

  inline CacheKey GetCacheKey(uint64_t file_number, uint64_t /*file_size*/,
                              uint64_t offset) const {
    // printf("DeltaLog GetCacheKey db_id_ = %s\n", db_id_.c_str());
    // printf("DeltaLog GetCacheKey db_session_id_ = %s\n",
    //        db_session_id_.c_str());
    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);
    return base_cache_key.WithOffset(offset);
  }

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
