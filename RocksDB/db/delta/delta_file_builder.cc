//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>

#include "db/delta/delta_contents.h"
#include "db/delta/delta_file_addition.h"
#include "db/delta/delta_file_builder.h"
#include "db/delta/delta_file_completion_callback.h"
#include "db/delta/delta_index.h"
#include "db/delta/delta_log_format.h"
#include "db/delta/delta_log_writer.h"
#include "db/event_helpers.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "trace_replay/io_tracer.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

DeltaFileBuilder::DeltaFileBuilder(
    VersionSet* versions, FileSystem* fs,
    const ImmutableOptions* immutable_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    std::string db_id, std::string db_session_id, int job_id,
    uint32_t column_family_id, const std::string& column_family_name,
    Env::IOPriority io_priority, Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    DeltaFileCompletionCallback* delta_callback,
    DeltaFileCreationReason creation_reason,
    std::vector<std::string>* delta_file_paths,
    std::vector<DeltaFileAddition>* delta_file_additions)
    : DeltaFileBuilder([versions]() { return versions->NewFileNumber(); }, fs,
                       immutable_options, mutable_cf_options, file_options,
                       db_id, db_session_id, job_id, column_family_id,
                       column_family_name, io_priority, write_hint, io_tracer,
                       delta_callback, creation_reason, delta_file_paths,
                       delta_file_additions) {}

DeltaFileBuilder::DeltaFileBuilder(
    std::function<uint64_t()> file_number_generator, FileSystem* fs,
    const ImmutableOptions* immutable_options,
    const MutableCFOptions* mutable_cf_options, const FileOptions* file_options,
    std::string db_id, std::string db_session_id, int job_id,
    uint32_t column_family_id, const std::string& column_family_name,
    Env::IOPriority io_priority, Env::WriteLifeTimeHint write_hint,
    const std::shared_ptr<IOTracer>& io_tracer,
    DeltaFileCompletionCallback* delta_callback,
    DeltaFileCreationReason creation_reason,
    std::vector<std::string>* delta_file_paths,
    std::vector<DeltaFileAddition>* delta_file_additions)
    : file_number_generator_(std::move(file_number_generator)),
      fs_(fs),
      immutable_options_(immutable_options),
      min_delta_size_(mutable_cf_options->min_delta_size),
      delta_file_size_(mutable_cf_options->delta_file_size),
      delta_compression_type_(mutable_cf_options->delta_compression_type),
      prepopulate_delta_cache_(mutable_cf_options->prepopulate_delta_cache),
      file_options_(file_options),
      db_id_(std::move(db_id)),
      db_session_id_(std::move(db_session_id)),
      job_id_(job_id),
      column_family_id_(column_family_id),
      column_family_name_(column_family_name),
      io_priority_(io_priority),
      write_hint_(write_hint),
      io_tracer_(io_tracer),
      delta_callback_(delta_callback),
      creation_reason_(creation_reason),
      delta_file_paths_(delta_file_paths),
      delta_file_additions_(delta_file_additions),
      delta_count_(0),
      delta_bytes_(0) {
  assert(file_number_generator_);
  assert(fs_);
  assert(immutable_options_);
  assert(file_options_);
  assert(delta_file_paths_);
  assert(delta_file_paths_->empty());
  assert(delta_file_additions_);
  assert(delta_file_additions_->empty());
}

DeltaFileBuilder::~DeltaFileBuilder() = default;

Status DeltaFileBuilder::Add(const Slice& key, const Slice& value,
                             std::string* delta_index) {
  assert(delta_index);
  assert(delta_index->empty());

  if (value.size() < min_delta_size_) {
    return Status::OK();
  }

  {
    const Status s = OpenDeltaFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  Slice delta = value;
  std::string compressed_delta;

  {
    const Status s = CompressDeltaIfNeeded(&delta, &compressed_delta);
    if (!s.ok()) {
      return s;
    }
  }

  uint64_t delta_file_number = 0;
  uint64_t delta_offset = 0;

  {
    const Status s =
        WriteDeltaToFile(key, delta, &delta_file_number, &delta_offset);
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s = CloseDeltaFileIfNeeded();
    if (!s.ok()) {
      return s;
    }
  }

  {
    const Status s =
        PutDeltaIntoCacheIfNeeded(value, delta_file_number, delta_offset);
    if (!s.ok()) {
      ROCKS_LOG_WARN(immutable_options_->info_log,
                     "Failed to pre-populate the delta into delta cache: %s",
                     s.ToString().c_str());
    }
  }

  DeltaIndex::EncodeDelta(delta_index, delta_file_number, delta_offset,
                          delta.size(), delta_compression_type_);

  return Status::OK();
}

Status DeltaFileBuilder::Finish() {
  if (!IsDeltaFileOpen()) {
    return Status::OK();
  }

  return CloseDeltaFile();
}

bool DeltaFileBuilder::IsDeltaFileOpen() const { return !!writer_; }

Status DeltaFileBuilder::OpenDeltaFileIfNeeded() {
  if (IsDeltaFileOpen()) {
    return Status::OK();
  }

  assert(!delta_count_);
  assert(!delta_bytes_);

  assert(file_number_generator_);
  const uint64_t delta_file_number = file_number_generator_();

  assert(immutable_options_);
  assert(!immutable_options_->cf_paths.empty());
  std::string delta_file_path = DeltaFileName(
      immutable_options_->cf_paths.front().path, delta_file_number);

  if (delta_callback_) {
    delta_callback_->OnDeltaFileCreationStarted(
        delta_file_path, column_family_name_, job_id_, creation_reason_);
  }

  std::unique_ptr<FSWritableFile> file;

  {
    assert(file_options_);
    Status s = NewWritableFile(fs_, delta_file_path, &file, *file_options_);

    TEST_SYNC_POINT_CALLBACK(
        "DeltaFileBuilder::OpenDeltaFileIfNeeded:NewWritableFile", &s);

    if (!s.ok()) {
      return s;
    }
  }

  // Note: files get added to delta_file_paths_ right after the open, so they
  // can be cleaned up upon failure. Contrast this with delta_file_additions_,
  // which only contains successfully written files.
  assert(delta_file_paths_);
  delta_file_paths_->emplace_back(std::move(delta_file_path));

  assert(file);
  file->SetIOPriority(io_priority_);
  file->SetWriteLifeTimeHint(write_hint_);
  FileTypeSet tmp_set = immutable_options_->checksum_handoff_file_types;
  Statistics* const statistics = immutable_options_->stats;
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), delta_file_paths_->back(), *file_options_,
      immutable_options_->clock, io_tracer_, statistics,
      immutable_options_->listeners,
      immutable_options_->file_checksum_gen_factory.get(),
      tmp_set.Contains(FileType::kDeltaFile), false));

  constexpr bool do_flush = false;

  std::unique_ptr<DeltaLogWriter> delta_log_writer(new DeltaLogWriter(
      std::move(file_writer), immutable_options_->clock, statistics,
      delta_file_number, immutable_options_->use_fsync, do_flush));

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  DeltaLogHeader header(column_family_id_, delta_compression_type_, has_ttl,
                        expiration_range);

  {
    Status s = delta_log_writer->WriteHeader(header);

    TEST_SYNC_POINT_CALLBACK(
        "DeltaFileBuilder::OpenDeltaFileIfNeeded:WriteHeader", &s);

    if (!s.ok()) {
      return s;
    }
  }

  writer_ = std::move(delta_log_writer);

  assert(IsDeltaFileOpen());

  return Status::OK();
}

Status DeltaFileBuilder::CompressDeltaIfNeeded(
    Slice* delta, std::string* compressed_delta) const {
  assert(delta);
  assert(compressed_delta);
  assert(compressed_delta->empty());
  assert(immutable_options_);

  if (delta_compression_type_ == kNoCompression) {
    return Status::OK();
  }

  CompressionOptions opts;
  CompressionContext context(delta_compression_type_);
  constexpr uint64_t sample_for_compression = 0;

  CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(),
                       delta_compression_type_, sample_for_compression);

  constexpr uint32_t compression_format_version = 2;

  bool success = false;

  {
    StopWatch stop_watch(immutable_options_->clock, immutable_options_->stats,
                         DELTA_DB_COMPRESSION_MICROS);
    success = CompressData(*delta, info, compression_format_version,
                           compressed_delta);
  }

  if (!success) {
    return Status::Corruption("Error compressing delta");
  }

  *delta = Slice(*compressed_delta);

  return Status::OK();
}

Status DeltaFileBuilder::WriteDeltaToFile(const Slice& key, const Slice& delta,
                                          uint64_t* delta_file_number,
                                          uint64_t* delta_offset) {
  assert(IsDeltaFileOpen());
  assert(delta_file_number);
  assert(delta_offset);

  uint64_t key_offset = 0;

  Status s = writer_->AddRecord(key, delta, &key_offset, delta_offset);

  TEST_SYNC_POINT_CALLBACK("DeltaFileBuilder::WriteDeltaToFile:AddRecord", &s);

  if (!s.ok()) {
    return s;
  }

  *delta_file_number = writer_->get_log_number();

  ++delta_count_;
  delta_bytes_ += DeltaLogRecord::kHeaderSize + key.size() + delta.size();

  return Status::OK();
}

Status DeltaFileBuilder::CloseDeltaFile() {
  assert(IsDeltaFileOpen());

  DeltaLogFooter footer;
  footer.delta_count = delta_count_;

  std::string checksum_method;
  std::string checksum_value;

  Status s = writer_->AppendFooter(footer, &checksum_method, &checksum_value);

  TEST_SYNC_POINT_CALLBACK("DeltaFileBuilder::WriteDeltaToFile:AppendFooter",
                           &s);

  if (!s.ok()) {
    return s;
  }

  const uint64_t delta_file_number = writer_->get_log_number();

  if (delta_callback_) {
    s = delta_callback_->OnDeltaFileCompleted(
        delta_file_paths_->back(), column_family_name_, job_id_,
        delta_file_number, creation_reason_, s, checksum_value, checksum_method,
        delta_count_, delta_bytes_);
  }

  assert(delta_file_additions_);
  delta_file_additions_->emplace_back(delta_file_number, delta_count_,
                                      delta_bytes_, std::move(checksum_method),
                                      std::move(checksum_value));

  assert(immutable_options_);
  ROCKS_LOG_INFO(immutable_options_->logger,
                 "[%s] [JOB %d] Generated delta file #%" PRIu64 ": %" PRIu64
                 " total deltas, %" PRIu64 " total bytes",
                 column_family_name_.c_str(), job_id_, delta_file_number,
                 delta_count_, delta_bytes_);

  writer_.reset();
  delta_count_ = 0;
  delta_bytes_ = 0;

  return s;
}

Status DeltaFileBuilder::CloseDeltaFileIfNeeded() {
  assert(IsDeltaFileOpen());

  const WritableFileWriter* const file_writer = writer_->file();
  assert(file_writer);

  if (file_writer->GetFileSize() < delta_file_size_) {
    return Status::OK();
  }

  return CloseDeltaFile();
}

void DeltaFileBuilder::Abandon(const Status& s) {
  if (!IsDeltaFileOpen()) {
    return;
  }
  if (delta_callback_) {
    // DeltaFileBuilder::Abandon() is called because of error while writing to
    // Delta files. So we can ignore the below error.
    delta_callback_
        ->OnDeltaFileCompleted(delta_file_paths_->back(), column_family_name_,
                               job_id_, writer_->get_log_number(),
                               creation_reason_, s, "", "", delta_count_,
                               delta_bytes_)
        .PermitUncheckedError();
  }

  writer_.reset();
  delta_count_ = 0;
  delta_bytes_ = 0;
}

Status DeltaFileBuilder::PutDeltaIntoCacheIfNeeded(
    const Slice& delta, uint64_t delta_file_number,
    uint64_t delta_offset) const {
  Status s = Status::OK();

  auto delta_cache = immutable_options_->delta_cache;
  auto statistics = immutable_options_->statistics.get();
  bool warm_cache =
      prepopulate_delta_cache_ == PrepopulateDeltaCache::kFlushOnly &&
      creation_reason_ == DeltaFileCreationReason::kFlush;

  if (delta_cache && warm_cache) {
    const OffsetableCacheKey base_cache_key(db_id_, db_session_id_,
                                            delta_file_number);
    const CacheKey cache_key = base_cache_key.WithOffset(delta_offset);
    const Slice key = cache_key.AsSlice();

    const Cache::Priority priority = Cache::Priority::BOTTOM;

    // Objects to be put into the cache have to be heap-allocated and
    // self-contained, i.e. own their contents. The Cache has to be able to
    // take unique ownership of them.
    CacheAllocationPtr allocation =
        AllocateBlock(delta.size(), delta_cache->memory_allocator());
    memcpy(allocation.get(), delta.data(), delta.size());
    std::unique_ptr<DeltaContents> buf =
        DeltaContents::Create(std::move(allocation), delta.size());

    Cache::CacheItemHelper* const cache_item_helper =
        DeltaContents::GetCacheItemHelper();
    assert(cache_item_helper);

    if (immutable_options_->lowest_used_cache_tier ==
        CacheTier::kNonVolatileBlockTier) {
      s = delta_cache->Insert(key, buf.get(), cache_item_helper,
                              buf->ApproximateMemoryUsage(),
                              nullptr /* cache_handle */, priority);
    } else {
      s = delta_cache->Insert(key, buf.get(), buf->ApproximateMemoryUsage(),
                              cache_item_helper->del_cb,
                              nullptr /* cache_handle */, priority);
    }

    if (s.ok()) {
      RecordTick(statistics, DELTA_DB_CACHE_ADD);
      RecordTick(statistics, DELTA_DB_CACHE_BYTES_WRITE, buf->size());
      buf.release();
    } else {
      RecordTick(statistics, DELTA_DB_CACHE_ADD_FAILURES);
    }
  }

  return s;
}

}  // namespace ROCKSDB_NAMESPACE
