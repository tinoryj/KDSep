//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cinttypes>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class VersionSet;
class FileSystem;
class SystemClock;
struct ImmutableOptions;
struct MutableCFOptions;
struct FileOptions;
class DeltaLogFileAddition;
class Status;
class Slice;
class DeltaLogLogWriter;
class IOTracer;
class DeltaLogFileCompletionCallback;

class DeltaLogFileBuilder {
 public:
  DeltaLogFileBuilder(
      VersionSet* versions, FileSystem* fs,
      const ImmutableOptions* immutable_options,
      const MutableCFOptions* mutable_cf_options,
      const FileOptions* file_options, std::string db_id,
      std::string db_session_id, int job_id, uint32_t column_family_id,
      const std::string& column_family_name, Env::IOPriority io_priority,
      Env::WriteLifeTimeHint write_hint,
      const std::shared_ptr<IOTracer>& io_tracer,
      DeltaLogFileCompletionCallback* deltaLog_callback,
      DeltaLogFileCreationReason creation_reason,
      std::vector<std::string>* deltaLog_file_paths,
      std::vector<DeltaLogFileAddition>* deltaLog_file_additions);

  DeltaLogFileBuilder(
      std::function<uint64_t()> file_number_generator, FileSystem* fs,
      const ImmutableOptions* immutable_options,
      const MutableCFOptions* mutable_cf_options,
      const FileOptions* file_options, std::string db_id,
      std::string db_session_id, int job_id, uint32_t column_family_id,
      const std::string& column_family_name, Env::IOPriority io_priority,
      Env::WriteLifeTimeHint write_hint,
      const std::shared_ptr<IOTracer>& io_tracer,
      DeltaLogFileCompletionCallback* deltaLog_callback,
      DeltaLogFileCreationReason creation_reason,
      std::vector<std::string>* deltaLog_file_paths,
      std::vector<DeltaLogFileAddition>* deltaLog_file_additions);

  DeltaLogFileBuilder(const DeltaLogFileBuilder&) = delete;
  DeltaLogFileBuilder& operator=(const DeltaLogFileBuilder&) = delete;

  ~DeltaLogFileBuilder();

  Status Add(const Slice& key, const Slice& value, std::string* deltaLog_index);
  Status Finish();
  void Abandon(const Status& s);

 private:
  bool IsDeltaLogFileOpen() const;
  Status OpenDeltaLogFileIfNeeded();
  Status CompressDeltaLogIfNeeded(Slice* deltaLog,
                                  std::string* compressed_deltaLog) const;
  Status WriteDeltaLogToFile(const Slice& key, const Slice& deltaLog,
                             uint64_t* deltaLog_file_number,
                             uint64_t* deltaLog_offset);
  Status CloseDeltaLogFile();
  Status CloseDeltaLogFileIfNeeded();

  Status PutDeltaLogIntoCacheIfNeeded(const Slice& deltaLog,
                                      uint64_t deltaLog_file_number,
                                      uint64_t deltaLog_offset) const;

  std::function<uint64_t()> file_number_generator_;
  FileSystem* fs_;
  const ImmutableOptions* immutable_options_;
  uint64_t min_deltaLog_size_;
  uint64_t deltaLog_file_size_;
  CompressionType deltaLog_compression_type_;
  PrepopulateBlobCache prepopulate_deltaLog_cache_;
  const FileOptions* file_options_;
  const std::string db_id_;
  const std::string db_session_id_;
  int job_id_;
  uint32_t column_family_id_;
  std::string column_family_name_;
  Env::IOPriority io_priority_;
  Env::WriteLifeTimeHint write_hint_;
  std::shared_ptr<IOTracer> io_tracer_;
  DeltaLogFileCompletionCallback* deltaLog_callback_;
  DeltaLogFileCreationReason creation_reason_;
  std::vector<std::string>* deltaLog_file_paths_;
  std::vector<DeltaLogFileAddition>* deltaLog_file_additions_;
  std::unique_ptr<DeltaLogLogWriter> writer_;
  uint64_t deltaLog_count_;
  uint64_t deltaLog_bytes_;
};

}  // namespace ROCKSDB_NAMESPACE
