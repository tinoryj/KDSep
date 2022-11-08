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
                               DeltaLogFileCache* deltaLog_file_cache)
    : db_id_(db_id),
      db_session_id_(db_session_id),
      statistics_(immutable_options->statistics.get()),
      deltaLog_file_cache_(deltaLog_file_cache),
      deltaLog_cache_(immutable_options->deltaLog_cache),
      lowest_used_cache_tier_(immutable_options->lowest_used_cache_tier) {
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

void DeltaLogSource::PinCachedDeltaLog(
    CacheHandleGuard<DeltaLogContents>* cached_deltaLog, PinnableSlice* value) {
  assert(cached_deltaLog);
  assert(cached_deltaLog->GetValue());
  assert(value);

  // To avoid copying the cached deltaLog into the buffer provided by the
  // application, we can simply transfer ownership of the cache handle to
  // the target PinnableSlice. This has the potential to save a lot of
  // CPU, especially with large deltaLog values.

  value->Reset();

  constexpr Cleanable* cleanable = nullptr;
  value->PinSlice(cached_deltaLog->GetValue()->data(), cleanable);

  cached_deltaLog->TransferTo(value);
}

void DeltaLogSource::PinOwnedDeltaLog(
    std::unique_ptr<DeltaLogContents>* owned_deltaLog, PinnableSlice* value) {
  assert(owned_deltaLog);
  assert(*owned_deltaLog);
  assert(value);

  DeltaLogContents* const deltaLog = owned_deltaLog->release();
  assert(deltaLog);

  value->Reset();
  value->PinSlice(
      deltaLog->data(),
      [](void* arg1, void* /* arg2 */) {
        delete static_cast<DeltaLogContents*>(arg1);
      },
      deltaLog, nullptr);
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
                                   const Slice& user_key, uint64_t file_number,
                                   uint64_t offset, uint64_t file_size,
                                   uint64_t value_size,
                                   CompressionType compression_type,
                                   FilePrefetchBuffer* prefetch_buffer,
                                   PinnableSlice* value, uint64_t* bytes_read) {
  assert(value);

  printf("Call get deltaLog function (deltaLog_source) for key = %s\n",
         user_key.ToString().c_str());

  Status s;

  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);

  CacheHandleGuard<DeltaLogContents> deltaLog_handle;

  // First, try to get the deltaLog from the cache
  //
  // If deltaLog cache is enabled, we'll try to read from it.
  if (deltaLog_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetDeltaLogFromCache(key, &deltaLog_handle);
    if (s.ok()) {
      PinCachedDeltaLog(&deltaLog_handle, value);

      // For consistency, the size of on-disk (possibly compressed) deltaLog
      // record is assigned to bytes_read.
      uint64_t adjustment =
          read_options.verify_checksums
              ? DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(
                    user_key.size())
              : 0;
      assert(offset >= adjustment);

      uint64_t record_size = value_size + adjustment;
      if (bytes_read) {
        *bytes_read = record_size;
      }
      return s;
    }
  }

  assert(deltaLog_handle.IsEmpty());

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
    s = deltaLog_file_cache_->GetDeltaLogFileReader(file_number,
                                                    &deltaLog_file_reader);
    if (!s.ok()) {
      return s;
    }

    assert(deltaLog_file_reader.GetValue());

    if (compression_type !=
        deltaLog_file_reader.GetValue()->GetCompressionType()) {
      return Status::Corruption(
          "Compression type mismatch when reading deltaLog");
    }

    MemoryAllocator* const allocator =
        (deltaLog_cache_ && read_options.fill_cache)
            ? deltaLog_cache_->memory_allocator()
            : nullptr;

    uint64_t read_size = 0;
    s = deltaLog_file_reader.GetValue()->GetDeltaLog(
        read_options, user_key, offset, value_size, compression_type,
        prefetch_buffer, allocator, &deltaLog_contents, &read_size);
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
    Slice key = cache_key.AsSlice();
    s = PutDeltaLogIntoCache(key, &deltaLog_contents, &deltaLog_handle);
    if (!s.ok()) {
      return s;
    }

    PinCachedDeltaLog(&deltaLog_handle, value);
  } else {
    PinOwnedDeltaLog(&deltaLog_contents, value);
  }

  assert(s.ok());
  return s;
}

void DeltaLogSource::MultiGetDeltaLog(
    const ReadOptions& read_options,
    autovector<DeltaLogFileReadRequests>& deltaLog_reqs, uint64_t* bytes_read) {
  assert(deltaLog_reqs.size() > 0);

  uint64_t total_bytes_read = 0;
  uint64_t bytes_read_in_file = 0;

  for (auto& [file_number, file_size, deltaLog_reqs_in_file] : deltaLog_reqs) {
    // sort deltaLog_reqs_in_file by file offset.
    std::sort(deltaLog_reqs_in_file.begin(), deltaLog_reqs_in_file.end(),
              [](const DeltaLogReadRequest& lhs, const DeltaLogReadRequest& rhs)
                  -> bool { return lhs.offset < rhs.offset; });

    MultiGetDeltaLogFromOneFile(read_options, file_number, file_size,
                                deltaLog_reqs_in_file, &bytes_read_in_file);

    total_bytes_read += bytes_read_in_file;
  }

  if (bytes_read) {
    *bytes_read = total_bytes_read;
  }
}

void DeltaLogSource::MultiGetDeltaLogFromOneFile(
    const ReadOptions& read_options, uint64_t file_number,
    uint64_t /*file_size*/, autovector<DeltaLogReadRequest>& deltaLog_reqs,
    uint64_t* bytes_read) {
  const size_t num_deltaLogs = deltaLog_reqs.size();
  assert(num_deltaLogs > 0);
  assert(num_deltaLogs <= MultiGetContext::MAX_BATCH_SIZE);

#ifndef NDEBUG
  for (size_t i = 0; i < num_deltaLogs - 1; ++i) {
    assert(deltaLog_reqs[i].offset <= deltaLog_reqs[i + 1].offset);
  }
#endif  // !NDEBUG

  using Mask = uint64_t;
  Mask cache_hit_mask = 0;

  uint64_t total_bytes = 0;
  const OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);

  if (deltaLog_cache_) {
    size_t cached_deltaLog_count = 0;
    for (size_t i = 0; i < num_deltaLogs; ++i) {
      auto& req = deltaLog_reqs[i];

      CacheHandleGuard<DeltaLogContents> deltaLog_handle;
      const CacheKey cache_key = base_cache_key.WithOffset(req.offset);
      const Slice key = cache_key.AsSlice();

      const Status s = GetDeltaLogFromCache(key, &deltaLog_handle);

      if (s.ok()) {
        assert(req.status);
        *req.status = s;

        PinCachedDeltaLog(&deltaLog_handle, req.result);

        // Update the counter for the number of valid deltaLogs read from the
        // cache.
        ++cached_deltaLog_count;

        // For consistency, the size of each on-disk (possibly compressed)
        // deltaLog record is accumulated to total_bytes.
        uint64_t adjustment =
            read_options.verify_checksums
                ? DeltaLogLogRecord::CalculateAdjustmentForRecordHeader(
                      req.user_key->size())
                : 0;
        assert(req.offset >= adjustment);
        total_bytes += req.len + adjustment;
        cache_hit_mask |= (Mask{1} << i);  // cache hit
      }
    }

    // All deltaLogs were read from the cache.
    if (cached_deltaLog_count == num_deltaLogs) {
      if (bytes_read) {
        *bytes_read = total_bytes;
      }
      return;
    }
  }

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    for (size_t i = 0; i < num_deltaLogs; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        DeltaLogReadRequest& req = deltaLog_reqs[i];
        assert(req.status);

        *req.status =
            Status::Incomplete("Cannot read deltaLog(s): no disk I/O allowed");
      }
    }
    return;
  }

  {
    // Find the rest of deltaLogs from the file since I/O is allowed.
    autovector<
        std::pair<DeltaLogReadRequest*, std::unique_ptr<DeltaLogContents>>>
        _deltaLog_reqs;
    uint64_t _bytes_read = 0;

    for (size_t i = 0; i < num_deltaLogs; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        _deltaLog_reqs.emplace_back(&deltaLog_reqs[i],
                                    std::unique_ptr<DeltaLogContents>());
      }
    }

    CacheHandleGuard<DeltaLogFileReader> deltaLog_file_reader;
    Status s = deltaLog_file_cache_->GetDeltaLogFileReader(
        file_number, &deltaLog_file_reader);
    if (!s.ok()) {
      for (size_t i = 0; i < _deltaLog_reqs.size(); ++i) {
        DeltaLogReadRequest* const req = _deltaLog_reqs[i].first;
        assert(req);
        assert(req->status);

        *req->status = s;
      }
      return;
    }

    assert(deltaLog_file_reader.GetValue());

    MemoryAllocator* const allocator =
        (deltaLog_cache_ && read_options.fill_cache)
            ? deltaLog_cache_->memory_allocator()
            : nullptr;

    deltaLog_file_reader.GetValue()->MultiGetDeltaLog(
        read_options, allocator, _deltaLog_reqs, &_bytes_read);

    if (deltaLog_cache_ && read_options.fill_cache) {
      // If filling cache is allowed and a cache is configured, try to put
      // the deltaLog(s) to the cache.
      for (auto& [req, deltaLog_contents] : _deltaLog_reqs) {
        assert(req);

        if (req->status->ok()) {
          CacheHandleGuard<DeltaLogContents> deltaLog_handle;
          const CacheKey cache_key = base_cache_key.WithOffset(req->offset);
          const Slice key = cache_key.AsSlice();
          s = PutDeltaLogIntoCache(key, &deltaLog_contents, &deltaLog_handle);
          if (!s.ok()) {
            *req->status = s;
          } else {
            PinCachedDeltaLog(&deltaLog_handle, req->result);
          }
        }
      }
    } else {
      for (auto& [req, deltaLog_contents] : _deltaLog_reqs) {
        assert(req);

        if (req->status->ok()) {
          PinOwnedDeltaLog(&deltaLog_contents, req->result);
        }
      }
    }

    total_bytes += _bytes_read;
    if (bytes_read) {
      *bytes_read = total_bytes;
    }
  }
}

bool DeltaLogSource::TEST_DeltaLogInCache(uint64_t file_number,
                                          uint64_t file_size, uint64_t offset,
                                          size_t* charge) const {
  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);
  const Slice key = cache_key.AsSlice();

  CacheHandleGuard<DeltaLogContents> deltaLog_handle;
  const Status s = GetDeltaLogFromCache(key, &deltaLog_handle);

  if (s.ok() && deltaLog_handle.GetValue() != nullptr) {
    if (charge) {
      const Cache* const cache = deltaLog_handle.GetCache();
      assert(cache);

      Cache::Handle* const handle = deltaLog_handle.GetCacheHandle();
      assert(handle);

      *charge = cache->GetUsage(handle);
    }

    return true;
  }

  return false;
}

}  // namespace ROCKSDB_NAMESPACE
