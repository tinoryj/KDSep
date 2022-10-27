//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <memory>

#include "db/delta/delta_file_cache.h"
#include "db/delta/delta_file_reader.h"
#include "options/cf_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "trace_replay/io_tracer.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

DeltaFileCache::DeltaFileCache(Cache* cache,
                             const ImmutableOptions* immutable_options,
                             const FileOptions* file_options,
                             uint32_t column_family_id,
                             HistogramImpl* delta_file_read_hist,
                             const std::shared_ptr<IOTracer>& io_tracer)
    : cache_(cache),
      mutex_(kNumberOfMutexStripes, kGetSliceNPHash64UnseededFnPtr),
      immutable_options_(immutable_options),
      file_options_(file_options),
      column_family_id_(column_family_id),
      delta_file_read_hist_(delta_file_read_hist),
      io_tracer_(io_tracer) {
  assert(cache_);
  assert(immutable_options_);
  assert(file_options_);
}

Status DeltaFileCache::GetDeltaFileReader(
    uint64_t delta_file_number,
    CacheHandleGuard<DeltaFileReader>* delta_file_reader) {
  assert(delta_file_reader);
  assert(delta_file_reader->IsEmpty());

  const Slice key = GetSlice(&delta_file_number);

  assert(cache_);

  Cache::Handle* handle = cache_->Lookup(key);
  if (handle) {
    *delta_file_reader = CacheHandleGuard<DeltaFileReader>(cache_, handle);
    return Status::OK();
  }

  TEST_SYNC_POINT("DeltaFileCache::GetDeltaFileReader:DoubleCheck");

  // Check again while holding mutex
  MutexLock lock(mutex_.get(key));

  handle = cache_->Lookup(key);
  if (handle) {
    *delta_file_reader = CacheHandleGuard<DeltaFileReader>(cache_, handle);
    return Status::OK();
  }

  assert(immutable_options_);
  Statistics* const statistics = immutable_options_->stats;

  RecordTick(statistics, NO_FILE_OPENS);

  std::unique_ptr<DeltaFileReader> reader;

  {
    assert(file_options_);
    const Status s = DeltaFileReader::Create(
        *immutable_options_, *file_options_, column_family_id_,
        delta_file_read_hist_, delta_file_number, io_tracer_, &reader);
    if (!s.ok()) {
      RecordTick(statistics, NO_FILE_ERRORS);
      return s;
    }
  }

  {
    constexpr size_t charge = 1;

    const Status s = cache_->Insert(key, reader.get(), charge,
                                    &DeleteCacheEntry<DeltaFileReader>, &handle);
    if (!s.ok()) {
      RecordTick(statistics, NO_FILE_ERRORS);
      return s;
    }
  }

  reader.release();

  *delta_file_reader = CacheHandleGuard<DeltaFileReader>(cache_, handle);

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
