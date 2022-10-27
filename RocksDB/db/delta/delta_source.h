//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cinttypes>
#include <memory>

#include "cache/cache_helpers.h"
#include "cache/cache_key.h"
#include "db/delta/delta_file_cache.h"
#include "db/delta/delta_read_request.h"
#include "rocksdb/cache.h"
#include "rocksdb/rocksdb_namespace.h"
#include "table/block_based/cachable_entry.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

struct ImmutableOptions;
class Status;
class FilePrefetchBuffer;
class Slice;
class DeltaContents;

// DeltaSource is a class that provides universal access to deltas, regardless
// of whether they are in the delta cache, secondary cache, or (remote) storage.
// Depending on user settings, it always fetch deltas from multi-tier cache and
// storage with minimal cost.
class DeltaSource {
 public:
  DeltaSource(const ImmutableOptions* immutable_options,
              const std::string& db_id, const std::string& db_session_id,
              DeltaFileCache* delta_file_cache);

  DeltaSource(const DeltaSource&) = delete;
  DeltaSource& operator=(const DeltaSource&) = delete;

  ~DeltaSource();

  // Read a delta from the underlying cache or one delta file.
  //
  // If successful, returns ok and sets "*value" to the newly retrieved
  // uncompressed delta. If there was an error while fetching the delta, sets
  // "*value" to empty and returns a non-ok status.
  //
  // Note: For consistency, whether the delta is found in the cache or on disk,
  // sets "*bytes_read" to the size of on-disk (possibly compressed) delta
  // record.
  Status GetDelta(const ReadOptions& read_options, const Slice& user_key,
                  uint64_t file_number, uint64_t offset, uint64_t file_size,
                  uint64_t value_size, CompressionType compression_type,
                  FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                  uint64_t* bytes_read);

  // Read multiple deltas from the underlying cache or delta file(s).
  //
  // If successful, returns ok and sets "result" in the elements of "delta_reqs"
  // to the newly retrieved uncompressed deltas. If there was an error while
  // fetching one of deltas, sets its "result" to empty and sets its
  // corresponding "status" to a non-ok status.
  //
  // Note:
  //  - The main difference between this function and MultiGetDeltaFromOneFile
  //  is
  //    that this function can read multiple deltas from multiple delta files.
  //
  //  - For consistency, whether the delta is found in the cache or on disk,
  //  sets
  //  "*bytes_read" to the total size of on-disk (possibly compressed) delta
  //  records.
  void MultiGetDelta(const ReadOptions& read_options,
                     autovector<DeltaFileReadRequests>& delta_reqs,
                     uint64_t* bytes_read);

  // Read multiple deltas from the underlying cache or one delta file.
  //
  // If successful, returns ok and sets "result" in the elements of "delta_reqs"
  // to the newly retrieved uncompressed deltas. If there was an error while
  // fetching one of deltas, sets its "result" to empty and sets its
  // corresponding "status" to a non-ok status.
  //
  // Note:
  //  - The main difference between this function and MultiGetDelta is that this
  //  function is only used for the case where the demanded deltas are stored in
  //  one delta file. MultiGetDelta will call this function multiple times if
  //  the demanded deltas are stored in multiple delta files.
  //
  //  - For consistency, whether the delta is found in the cache or on disk,
  //  sets
  //  "*bytes_read" to the total size of on-disk (possibly compressed) delta
  //  records.
  void MultiGetDeltaFromOneFile(const ReadOptions& read_options,
                                uint64_t file_number, uint64_t file_size,
                                autovector<DeltaReadRequest>& delta_reqs,
                                uint64_t* bytes_read);

  inline Status GetDeltaFileReader(
      uint64_t delta_file_number,
      CacheHandleGuard<DeltaFileReader>* delta_file_reader) {
    return delta_file_cache_->GetDeltaFileReader(delta_file_number,
                                                 delta_file_reader);
  }

  inline Cache* GetDeltaCache() const { return delta_cache_.get(); }

  bool TEST_DeltaInCache(uint64_t file_number, uint64_t file_size,
                         uint64_t offset, size_t* charge = nullptr) const;

 private:
  Status GetDeltaFromCache(const Slice& cache_key,
                           CacheHandleGuard<DeltaContents>* cached_delta) const;

  Status PutDeltaIntoCache(const Slice& cache_key,
                           std::unique_ptr<DeltaContents>* delta,
                           CacheHandleGuard<DeltaContents>* cached_delta) const;

  static void PinCachedDelta(CacheHandleGuard<DeltaContents>* cached_delta,
                             PinnableSlice* value);

  static void PinOwnedDelta(std::unique_ptr<DeltaContents>* owned_delta,
                            PinnableSlice* value);

  Cache::Handle* GetEntryFromCache(const Slice& key) const;

  Status InsertEntryIntoCache(const Slice& key, DeltaContents* value,
                              size_t charge, Cache::Handle** cache_handle,
                              Cache::Priority priority) const;

  inline CacheKey GetCacheKey(uint64_t file_number, uint64_t /*file_size*/,
                              uint64_t offset) const {
    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);
    return base_cache_key.WithOffset(offset);
  }

  const std::string& db_id_;
  const std::string& db_session_id_;

  Statistics* statistics_;

  // A cache to store delta file reader.
  DeltaFileCache* delta_file_cache_;

  // A cache to store uncompressed deltas.
  std::shared_ptr<Cache> delta_cache_;

  // The control option of how the cache tiers will be used. Currently rocksdb
  // support block/delta cache (volatile tier) and secondary cache (this tier
  // isn't strictly speaking a non-volatile tier since the compressed cache in
  // this tier is in volatile memory).
  const CacheTier lowest_used_cache_tier_;
};

}  // namespace ROCKSDB_NAMESPACE
