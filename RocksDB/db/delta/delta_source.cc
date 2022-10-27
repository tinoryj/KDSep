//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <string>

#include "cache/cache_reservation_manager.h"
#include "cache/charged_cache.h"
#include "db/delta/delta_contents.h"
#include "db/delta/delta_file_reader.h"
#include "db/delta/delta_log_format.h"
#include "db/delta/delta_source.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"
#include "table/get_context.h"
#include "table/multiget_context.h"

namespace ROCKSDB_NAMESPACE {

DeltaSource::DeltaSource(const ImmutableOptions* immutable_options,
                         const std::string& db_id,
                         const std::string& db_session_id,
                         DeltaFileCache* delta_file_cache)
    : db_id_(db_id),
      db_session_id_(db_session_id),
      statistics_(immutable_options->statistics.get()),
      delta_file_cache_(delta_file_cache),
      delta_cache_(immutable_options->delta_cache),
      lowest_used_cache_tier_(immutable_options->lowest_used_cache_tier) {
#ifndef ROCKSDB_LITE
  auto bbto =
      immutable_options->table_factory->GetOptions<BlockBasedTableOptions>();
  if (bbto && bbto->cache_usage_options.options_overrides
                      .at(CacheEntryRole::kDeltaCache)
                      .charged == CacheEntryRoleOptions::Decision::kEnabled) {
    delta_cache_ = std::make_shared<ChargedCache>(
        immutable_options->delta_cache, bbto->block_cache);
  }
#endif  // ROCKSDB_LITE
}

DeltaSource::~DeltaSource() = default;

Status DeltaSource::GetDeltaFromCache(
    const Slice& cache_key,
    CacheHandleGuard<DeltaContents>* cached_delta) const {
  assert(delta_cache_);
  assert(!cache_key.empty());
  assert(cached_delta);
  assert(cached_delta->IsEmpty());

  Cache::Handle* cache_handle = nullptr;
  cache_handle = GetEntryFromCache(cache_key);
  if (cache_handle != nullptr) {
    *cached_delta =
        CacheHandleGuard<DeltaContents>(delta_cache_.get(), cache_handle);

    assert(cached_delta->GetValue());

    PERF_COUNTER_ADD(delta_cache_hit_count, 1);
    RecordTick(statistics_, DELTA_DB_CACHE_HIT);
    RecordTick(statistics_, DELTA_DB_CACHE_BYTES_READ,
               cached_delta->GetValue()->size());

    return Status::OK();
  }

  RecordTick(statistics_, DELTA_DB_CACHE_MISS);

  return Status::NotFound("Delta not found in cache");
}

Status DeltaSource::PutDeltaIntoCache(
    const Slice& cache_key, std::unique_ptr<DeltaContents>* delta,
    CacheHandleGuard<DeltaContents>* cached_delta) const {
  assert(delta_cache_);
  assert(!cache_key.empty());
  assert(delta);
  assert(*delta);
  assert(cached_delta);
  assert(cached_delta->IsEmpty());

  Cache::Handle* cache_handle = nullptr;
  const Status s = InsertEntryIntoCache(cache_key, delta->get(),
                                        (*delta)->ApproximateMemoryUsage(),
                                        &cache_handle, Cache::Priority::BOTTOM);
  if (s.ok()) {
    delta->release();

    assert(cache_handle != nullptr);
    *cached_delta =
        CacheHandleGuard<DeltaContents>(delta_cache_.get(), cache_handle);

    assert(cached_delta->GetValue());

    RecordTick(statistics_, DELTA_DB_CACHE_ADD);
    RecordTick(statistics_, DELTA_DB_CACHE_BYTES_WRITE,
               cached_delta->GetValue()->size());

  } else {
    RecordTick(statistics_, DELTA_DB_CACHE_ADD_FAILURES);
  }

  return s;
}

Cache::Handle* DeltaSource::GetEntryFromCache(const Slice& key) const {
  Cache::Handle* cache_handle = nullptr;

  if (lowest_used_cache_tier_ == CacheTier::kNonVolatileBlockTier) {
    Cache::CreateCallback create_cb =
        [allocator = delta_cache_->memory_allocator()](
            const void* buf, size_t size, void** out_obj,
            size_t* charge) -> Status {
      return DeltaContents::CreateCallback(AllocateBlock(size, allocator), buf,
                                           size, out_obj, charge);
    };

    cache_handle = delta_cache_->Lookup(
        key, DeltaContents::GetCacheItemHelper(), create_cb,
        Cache::Priority::BOTTOM, true /* wait_for_cache */, statistics_);
  } else {
    cache_handle = delta_cache_->Lookup(key, statistics_);
  }

  return cache_handle;
}

void DeltaSource::PinCachedDelta(CacheHandleGuard<DeltaContents>* cached_delta,
                                 PinnableSlice* value) {
  assert(cached_delta);
  assert(cached_delta->GetValue());
  assert(value);

  // To avoid copying the cached delta into the buffer provided by the
  // application, we can simply transfer ownership of the cache handle to
  // the target PinnableSlice. This has the potential to save a lot of
  // CPU, especially with large delta values.

  value->Reset();

  constexpr Cleanable* cleanable = nullptr;
  value->PinSlice(cached_delta->GetValue()->data(), cleanable);

  cached_delta->TransferTo(value);
}

void DeltaSource::PinOwnedDelta(std::unique_ptr<DeltaContents>* owned_delta,
                                PinnableSlice* value) {
  assert(owned_delta);
  assert(*owned_delta);
  assert(value);

  DeltaContents* const delta = owned_delta->release();
  assert(delta);

  value->Reset();
  value->PinSlice(
      delta->data(),
      [](void* arg1, void* /* arg2 */) {
        delete static_cast<DeltaContents*>(arg1);
      },
      delta, nullptr);
}

Status DeltaSource::InsertEntryIntoCache(const Slice& key, DeltaContents* value,
                                         size_t charge,
                                         Cache::Handle** cache_handle,
                                         Cache::Priority priority) const {
  Status s;

  Cache::CacheItemHelper* const cache_item_helper =
      DeltaContents::GetCacheItemHelper();
  assert(cache_item_helper);

  if (lowest_used_cache_tier_ == CacheTier::kNonVolatileBlockTier) {
    s = delta_cache_->Insert(key, value, cache_item_helper, charge,
                             cache_handle, priority);
  } else {
    s = delta_cache_->Insert(key, value, charge, cache_item_helper->del_cb,
                             cache_handle, priority);
  }

  return s;
}

Status DeltaSource::GetDelta(const ReadOptions& read_options,
                             const Slice& user_key, uint64_t file_number,
                             uint64_t offset, uint64_t file_size,
                             uint64_t value_size,
                             CompressionType compression_type,
                             FilePrefetchBuffer* prefetch_buffer,
                             PinnableSlice* value, uint64_t* bytes_read) {
  assert(value);

  Status s;

  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);

  CacheHandleGuard<DeltaContents> delta_handle;

  // First, try to get the delta from the cache
  //
  // If delta cache is enabled, we'll try to read from it.
  if (delta_cache_) {
    Slice key = cache_key.AsSlice();
    s = GetDeltaFromCache(key, &delta_handle);
    if (s.ok()) {
      PinCachedDelta(&delta_handle, value);

      // For consistency, the size of on-disk (possibly compressed) delta record
      // is assigned to bytes_read.
      uint64_t adjustment =
          read_options.verify_checksums
              ? DeltaLogRecord::CalculateAdjustmentForRecordHeader(
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

  assert(delta_handle.IsEmpty());

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    s = Status::Incomplete("Cannot read delta(s): no disk I/O allowed");
    return s;
  }

  // Can't find the delta from the cache. Since I/O is allowed, read from the
  // file.
  std::unique_ptr<DeltaContents> delta_contents;

  {
    CacheHandleGuard<DeltaFileReader> delta_file_reader;
    s = delta_file_cache_->GetDeltaFileReader(file_number, &delta_file_reader);
    if (!s.ok()) {
      return s;
    }

    assert(delta_file_reader.GetValue());

    if (compression_type !=
        delta_file_reader.GetValue()->GetCompressionType()) {
      return Status::Corruption("Compression type mismatch when reading delta");
    }

    MemoryAllocator* const allocator = (delta_cache_ && read_options.fill_cache)
                                           ? delta_cache_->memory_allocator()
                                           : nullptr;

    uint64_t read_size = 0;
    s = delta_file_reader.GetValue()->GetDelta(
        read_options, user_key, offset, value_size, compression_type,
        prefetch_buffer, allocator, &delta_contents, &read_size);
    if (!s.ok()) {
      return s;
    }
    if (bytes_read) {
      *bytes_read = read_size;
    }
  }

  if (delta_cache_ && read_options.fill_cache) {
    // If filling cache is allowed and a cache is configured, try to put the
    // delta to the cache.
    Slice key = cache_key.AsSlice();
    s = PutDeltaIntoCache(key, &delta_contents, &delta_handle);
    if (!s.ok()) {
      return s;
    }

    PinCachedDelta(&delta_handle, value);
  } else {
    PinOwnedDelta(&delta_contents, value);
  }

  assert(s.ok());
  return s;
}

void DeltaSource::MultiGetDelta(const ReadOptions& read_options,
                                autovector<DeltaFileReadRequests>& delta_reqs,
                                uint64_t* bytes_read) {
  assert(delta_reqs.size() > 0);

  uint64_t total_bytes_read = 0;
  uint64_t bytes_read_in_file = 0;

  for (auto& [file_number, file_size, delta_reqs_in_file] : delta_reqs) {
    // sort delta_reqs_in_file by file offset.
    std::sort(
        delta_reqs_in_file.begin(), delta_reqs_in_file.end(),
        [](const DeltaReadRequest& lhs, const DeltaReadRequest& rhs) -> bool {
          return lhs.offset < rhs.offset;
        });

    MultiGetDeltaFromOneFile(read_options, file_number, file_size,
                             delta_reqs_in_file, &bytes_read_in_file);

    total_bytes_read += bytes_read_in_file;
  }

  if (bytes_read) {
    *bytes_read = total_bytes_read;
  }
}

void DeltaSource::MultiGetDeltaFromOneFile(
    const ReadOptions& read_options, uint64_t file_number,
    uint64_t /*file_size*/, autovector<DeltaReadRequest>& delta_reqs,
    uint64_t* bytes_read) {
  const size_t num_deltas = delta_reqs.size();
  assert(num_deltas > 0);
  assert(num_deltas <= MultiGetContext::MAX_BATCH_SIZE);

#ifndef NDEBUG
  for (size_t i = 0; i < num_deltas - 1; ++i) {
    assert(delta_reqs[i].offset <= delta_reqs[i + 1].offset);
  }
#endif  // !NDEBUG

  using Mask = uint64_t;
  Mask cache_hit_mask = 0;

  uint64_t total_bytes = 0;
  const OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);

  if (delta_cache_) {
    size_t cached_delta_count = 0;
    for (size_t i = 0; i < num_deltas; ++i) {
      auto& req = delta_reqs[i];

      CacheHandleGuard<DeltaContents> delta_handle;
      const CacheKey cache_key = base_cache_key.WithOffset(req.offset);
      const Slice key = cache_key.AsSlice();

      const Status s = GetDeltaFromCache(key, &delta_handle);

      if (s.ok()) {
        assert(req.status);
        *req.status = s;

        PinCachedDelta(&delta_handle, req.result);

        // Update the counter for the number of valid deltas read from the
        // cache.
        ++cached_delta_count;

        // For consistency, the size of each on-disk (possibly compressed) delta
        // record is accumulated to total_bytes.
        uint64_t adjustment =
            read_options.verify_checksums
                ? DeltaLogRecord::CalculateAdjustmentForRecordHeader(
                      req.user_key->size())
                : 0;
        assert(req.offset >= adjustment);
        total_bytes += req.len + adjustment;
        cache_hit_mask |= (Mask{1} << i);  // cache hit
      }
    }

    // All deltas were read from the cache.
    if (cached_delta_count == num_deltas) {
      if (bytes_read) {
        *bytes_read = total_bytes;
      }
      return;
    }
  }

  const bool no_io = read_options.read_tier == kBlockCacheTier;
  if (no_io) {
    for (size_t i = 0; i < num_deltas; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        DeltaReadRequest& req = delta_reqs[i];
        assert(req.status);

        *req.status =
            Status::Incomplete("Cannot read delta(s): no disk I/O allowed");
      }
    }
    return;
  }

  {
    // Find the rest of deltas from the file since I/O is allowed.
    autovector<std::pair<DeltaReadRequest*, std::unique_ptr<DeltaContents>>>
        _delta_reqs;
    uint64_t _bytes_read = 0;

    for (size_t i = 0; i < num_deltas; ++i) {
      if (!(cache_hit_mask & (Mask{1} << i))) {
        _delta_reqs.emplace_back(&delta_reqs[i],
                                 std::unique_ptr<DeltaContents>());
      }
    }

    CacheHandleGuard<DeltaFileReader> delta_file_reader;
    Status s =
        delta_file_cache_->GetDeltaFileReader(file_number, &delta_file_reader);
    if (!s.ok()) {
      for (size_t i = 0; i < _delta_reqs.size(); ++i) {
        DeltaReadRequest* const req = _delta_reqs[i].first;
        assert(req);
        assert(req->status);

        *req->status = s;
      }
      return;
    }

    assert(delta_file_reader.GetValue());

    MemoryAllocator* const allocator = (delta_cache_ && read_options.fill_cache)
                                           ? delta_cache_->memory_allocator()
                                           : nullptr;

    delta_file_reader.GetValue()->MultiGetDelta(read_options, allocator,
                                                _delta_reqs, &_bytes_read);

    if (delta_cache_ && read_options.fill_cache) {
      // If filling cache is allowed and a cache is configured, try to put
      // the delta(s) to the cache.
      for (auto& [req, delta_contents] : _delta_reqs) {
        assert(req);

        if (req->status->ok()) {
          CacheHandleGuard<DeltaContents> delta_handle;
          const CacheKey cache_key = base_cache_key.WithOffset(req->offset);
          const Slice key = cache_key.AsSlice();
          s = PutDeltaIntoCache(key, &delta_contents, &delta_handle);
          if (!s.ok()) {
            *req->status = s;
          } else {
            PinCachedDelta(&delta_handle, req->result);
          }
        }
      }
    } else {
      for (auto& [req, delta_contents] : _delta_reqs) {
        assert(req);

        if (req->status->ok()) {
          PinOwnedDelta(&delta_contents, req->result);
        }
      }
    }

    total_bytes += _bytes_read;
    if (bytes_read) {
      *bytes_read = total_bytes;
    }
  }
}

bool DeltaSource::TEST_DeltaInCache(uint64_t file_number, uint64_t file_size,
                                    uint64_t offset, size_t* charge) const {
  const CacheKey cache_key = GetCacheKey(file_number, file_size, offset);
  const Slice key = cache_key.AsSlice();

  CacheHandleGuard<DeltaContents> delta_handle;
  const Status s = GetDeltaFromCache(key, &delta_handle);

  if (s.ok() && delta_handle.GetValue() != nullptr) {
    if (charge) {
      const Cache* const cache = delta_handle.GetCache();
      assert(cache);

      Cache::Handle* const handle = delta_handle.GetCacheHandle();
      assert(handle);

      *charge = cache->GetUsage(handle);
    }

    return true;
  }

  return false;
}

}  // namespace ROCKSDB_NAMESPACE
