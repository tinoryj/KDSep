//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/delta/delta_fetcher.h"
#include "db/version_set.h"

namespace ROCKSDB_NAMESPACE {

Status DeltaFetcher::FetchDelta(const Slice& user_key,
                                const Slice& delta_index_slice,
                                FilePrefetchBuffer* prefetch_buffer,
                                PinnableSlice* delta_value,
                                uint64_t* bytes_read) const {
  assert(version_);

  return version_->GetDelta(read_options_, user_key, delta_index_slice,
                            prefetch_buffer, delta_value, bytes_read);
}

Status DeltaFetcher::FetchDelta(const Slice& user_key,
                                const DeltaIndex& delta_index,
                                FilePrefetchBuffer* prefetch_buffer,
                                PinnableSlice* delta_value,
                                uint64_t* bytes_read) const {
  assert(version_);

  return version_->GetDelta(read_options_, user_key, delta_index,
                            prefetch_buffer, delta_value, bytes_read);
}

}  // namespace ROCKSDB_NAMESPACE
