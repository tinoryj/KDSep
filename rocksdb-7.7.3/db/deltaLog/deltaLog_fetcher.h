//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class Version;
class Slice;
class FilePrefetchBuffer;
class PinnableSlice;
class DeltaLogIndex;

// A thin wrapper around the deltaLog retrieval functionality of Version.
class DeltaLogFetcher {
 public:
  DeltaLogFetcher(const Version* version, const ReadOptions& read_options)
      : version_(version), read_options_(read_options) {}

  Status FetchDeltaLog(const Slice& user_key,
                       FilePrefetchBuffer* prefetch_buffer,
                       autovector<Slice>& deltaLog_value_vec,
                       uint64_t* bytes_read) const;

 private:
  const Version* version_;
  ReadOptions read_options_;
};
}  // namespace ROCKSDB_NAMESPACE
