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
class DeltaFileAddition;
class Status;
class Slice;
class DeltaLogWriter;
class IOTracer;
class DeltaFileCompletionCallback;

class DeltaFileBuilder {
 public:
  DeltaFileBuilder(VersionSet* versions, FileSystem* fs,
                   const ImmutableOptions* immutable_options,
                   const MutableCFOptions* mutable_cf_options,
                   const FileOptions* file_options, std::string db_id,
                   std::string db_session_id, int job_id,
                   uint32_t column_family_id,
                   const std::string& column_family_name,
                   Env::IOPriority io_priority,
                   Env::WriteLifeTimeHint write_hint,
                   const std::shared_ptr<IOTracer>& io_tracer,
                   DeltaFileCompletionCallback* delta_callback,
                   DeltaFileCreationReason creation_reason,
                   std::vector<std::string>* delta_file_paths,
                   std::vector<DeltaFileAddition>* delta_file_additions);

  DeltaFileBuilder(std::function<uint64_t()> file_number_generator,
                   FileSystem* fs, const ImmutableOptions* immutable_options,
                   const MutableCFOptions* mutable_cf_options,
                   const FileOptions* file_options, std::string db_id,
                   std::string db_session_id, int job_id,
                   uint32_t column_family_id,
                   const std::string& column_family_name,
                   Env::IOPriority io_priority,
                   Env::WriteLifeTimeHint write_hint,
                   const std::shared_ptr<IOTracer>& io_tracer,
                   DeltaFileCompletionCallback* delta_callback,
                   DeltaFileCreationReason creation_reason,
                   std::vector<std::string>* delta_file_paths,
                   std::vector<DeltaFileAddition>* delta_file_additions);

  DeltaFileBuilder(const DeltaFileBuilder&) = delete;
  DeltaFileBuilder& operator=(const DeltaFileBuilder&) = delete;

  ~DeltaFileBuilder();

  Status Add(const Slice& key, const Slice& value, std::string* delta_index);
  Status Finish();
  void Abandon(const Status& s);

 private:
  bool IsDeltaFileOpen() const;
  Status OpenDeltaFileIfNeeded();
  Status CompressDeltaIfNeeded(Slice* delta,
                               std::string* compressed_delta) const;
  Status WriteDeltaToFile(const Slice& key, const Slice& delta,
                          uint64_t* delta_file_number, uint64_t* delta_offset);
  Status CloseDeltaFile();
  Status CloseDeltaFileIfNeeded();

  Status PutDeltaIntoCacheIfNeeded(const Slice& delta,
                                   uint64_t delta_file_number,
                                   uint64_t delta_offset) const;

  std::function<uint64_t()> file_number_generator_;
  FileSystem* fs_;
  const ImmutableOptions* immutable_options_;
  uint64_t min_delta_size_;
  uint64_t delta_file_size_;
  CompressionType delta_compression_type_;
  PrepopulateDeltaCache prepopulate_delta_cache_;
  const FileOptions* file_options_;
  const std::string db_id_;
  const std::string db_session_id_;
  int job_id_;
  uint32_t column_family_id_;
  std::string column_family_name_;
  Env::IOPriority io_priority_;
  Env::WriteLifeTimeHint write_hint_;
  std::shared_ptr<IOTracer> io_tracer_;
  DeltaFileCompletionCallback* delta_callback_;
  DeltaFileCreationReason creation_reason_;
  std::vector<std::string>* delta_file_paths_;
  std::vector<DeltaFileAddition>* delta_file_additions_;
  std::unique_ptr<DeltaLogWriter> writer_;
  uint64_t delta_count_;
  uint64_t delta_bytes_;
};

}  // namespace ROCKSDB_NAMESPACE
