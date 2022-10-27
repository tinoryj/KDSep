//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/delta_db/delta_compaction_filter.h"

#include <cinttypes>

#include "db/dbformat.h"
#include "logging/logging.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
namespace delta_db {

DeltaIndexCompactionFilterBase::~DeltaIndexCompactionFilterBase() {
  if (delta_file_) {
    CloseAndRegisterNewDeltaFile();
  }
  RecordTick(statistics_, DELTA_DB_DELTA_INDEX_EXPIRED_COUNT, expired_count_);
  RecordTick(statistics_, DELTA_DB_DELTA_INDEX_EXPIRED_SIZE, expired_size_);
  RecordTick(statistics_, DELTA_DB_DELTA_INDEX_EVICTED_COUNT, evicted_count_);
  RecordTick(statistics_, DELTA_DB_DELTA_INDEX_EVICTED_SIZE, evicted_size_);
}

CompactionFilter::Decision DeltaIndexCompactionFilterBase::FilterV2(
    int level, const Slice& key, ValueType value_type, const Slice& value,
    std::string* new_value, std::string* skip_until) const {
  const CompactionFilter* ucf = user_comp_filter();
  if (value_type != kDeltaIndex) {
    if (ucf == nullptr) {
      return Decision::kKeep;
    }
    // Apply user compaction filter for inlined data.
    CompactionFilter::Decision decision =
        ucf->FilterV2(level, key, value_type, value, new_value, skip_until);
    if (decision == Decision::kChangeValue) {
      return HandleValueChange(key, new_value);
    }
    return decision;
  }
  DeltaIndex delta_index;
  Status s = delta_index.DecodeFrom(value);
  if (!s.ok()) {
    // Unable to decode delta index. Keeping the value.
    return Decision::kKeep;
  }
  if (delta_index.HasTTL() && delta_index.expiration() <= current_time_) {
    // Expired
    expired_count_++;
    expired_size_ += key.size() + value.size();
    return Decision::kRemove;
  }
  if (!delta_index.IsInlined() &&
      delta_index.file_number() < context_.next_file_number &&
      context_.current_delta_files.count(delta_index.file_number()) == 0) {
    // Corresponding delta file gone (most likely, evicted by FIFO eviction).
    evicted_count_++;
    evicted_size_ += key.size() + value.size();
    return Decision::kRemove;
  }
  if (context_.fifo_eviction_seq > 0 && delta_index.HasTTL() &&
      delta_index.expiration() < context_.evict_expiration_up_to) {
    // Hack: Internal key is passed to DeltaIndexCompactionFilter for it to
    // get sequence number.
    ParsedInternalKey ikey;
    if (!ParseInternalKey(
             key, &ikey,
             context_.delta_db_impl->db_options_.allow_data_in_errors)
             .ok()) {
      assert(false);
      return Decision::kKeep;
    }
    // Remove keys that could have been remove by last FIFO eviction.
    // If get error while parsing key, ignore and continue.
    if (ikey.sequence < context_.fifo_eviction_seq) {
      evicted_count_++;
      evicted_size_ += key.size() + value.size();
      return Decision::kRemove;
    }
  }
  // Apply user compaction filter for all non-TTL delta data.
  if (ucf != nullptr && !delta_index.HasTTL()) {
    // Hack: Internal key is passed to DeltaIndexCompactionFilter for it to
    // get sequence number.
    ParsedInternalKey ikey;
    if (!ParseInternalKey(
             key, &ikey,
             context_.delta_db_impl->db_options_.allow_data_in_errors)
             .ok()) {
      assert(false);
      return Decision::kKeep;
    }
    // Read value from delta file.
    PinnableSlice delta;
    CompressionType compression_type = kNoCompression;
    constexpr bool need_decompress = true;
    if (!ReadDeltaFromOldFile(ikey.user_key, delta_index, &delta,
                              need_decompress, &compression_type)) {
      return Decision::kIOError;
    }
    CompactionFilter::Decision decision = ucf->FilterV2(
        level, ikey.user_key, kValue, delta, new_value, skip_until);
    if (decision == Decision::kChangeValue) {
      return HandleValueChange(ikey.user_key, new_value);
    }
    return decision;
  }
  return Decision::kKeep;
}

CompactionFilter::Decision DeltaIndexCompactionFilterBase::HandleValueChange(
    const Slice& key, std::string* new_value) const {
  DeltaDBImpl* const delta_db_impl = context_.delta_db_impl;
  assert(delta_db_impl);

  if (new_value->size() < delta_db_impl->bdb_options_.min_delta_size) {
    // Keep new_value inlined.
    return Decision::kChangeValue;
  }
  if (!OpenNewDeltaFileIfNeeded()) {
    return Decision::kIOError;
  }
  Slice new_delta_value(*new_value);
  std::string compression_output;
  if (delta_db_impl->bdb_options_.compression != kNoCompression) {
    new_delta_value =
        delta_db_impl->GetCompressedSlice(new_delta_value, &compression_output);
  }
  uint64_t new_delta_file_number = 0;
  uint64_t new_delta_offset = 0;
  if (!WriteDeltaToNewFile(key, new_delta_value, &new_delta_file_number,
                           &new_delta_offset)) {
    return Decision::kIOError;
  }
  if (!CloseAndRegisterNewDeltaFileIfNeeded()) {
    return Decision::kIOError;
  }
  DeltaIndex::EncodeDelta(new_value, new_delta_file_number, new_delta_offset,
                          new_delta_value.size(),
                          delta_db_impl->bdb_options_.compression);
  return Decision::kChangeDeltaIndex;
}

DeltaIndexCompactionFilterGC::~DeltaIndexCompactionFilterGC() {
  assert(context().delta_db_impl);

  ROCKS_LOG_INFO(context().delta_db_impl->db_options_.info_log,
                 "GC pass finished %s: encountered %" PRIu64 " deltas (%" PRIu64
                 " bytes), relocated %" PRIu64 " deltas (%" PRIu64
                 " bytes), created %" PRIu64 " new delta file(s)",
                 !gc_stats_.HasError() ? "successfully" : "with failure",
                 gc_stats_.AllDeltas(), gc_stats_.AllBytes(),
                 gc_stats_.RelocatedDeltas(), gc_stats_.RelocatedBytes(),
                 gc_stats_.NewFiles());

  RecordTick(statistics(), DELTA_DB_GC_NUM_KEYS_RELOCATED,
             gc_stats_.RelocatedDeltas());
  RecordTick(statistics(), DELTA_DB_GC_BYTES_RELOCATED,
             gc_stats_.RelocatedBytes());
  RecordTick(statistics(), DELTA_DB_GC_NUM_NEW_FILES, gc_stats_.NewFiles());
  RecordTick(statistics(), DELTA_DB_GC_FAILURES, gc_stats_.HasError());
}

bool DeltaIndexCompactionFilterBase::IsDeltaFileOpened() const {
  if (delta_file_) {
    assert(writer_);
    return true;
  }
  return false;
}

bool DeltaIndexCompactionFilterBase::OpenNewDeltaFileIfNeeded() const {
  if (IsDeltaFileOpened()) {
    return true;
  }

  DeltaDBImpl* const delta_db_impl = context_.delta_db_impl;
  assert(delta_db_impl);

  const Status s = delta_db_impl->CreateDeltaFileAndWriter(
      /* has_ttl */ false, ExpirationRange(), "compaction/GC", &delta_file_,
      &writer_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        delta_db_impl->db_options_.info_log,
        "Error opening new delta file during compaction/GC, status: %s",
        s.ToString().c_str());
    delta_file_.reset();
    writer_.reset();
    return false;
  }

  assert(delta_file_);
  assert(writer_);

  return true;
}

bool DeltaIndexCompactionFilterBase::ReadDeltaFromOldFile(
    const Slice& key, const DeltaIndex& delta_index, PinnableSlice* delta,
    bool need_decompress, CompressionType* compression_type) const {
  DeltaDBImpl* const delta_db_impl = context_.delta_db_impl;
  assert(delta_db_impl);

  Status s = delta_db_impl->GetRawDeltaFromFile(
      key, delta_index.file_number(), delta_index.offset(), delta_index.size(),
      delta, compression_type);

  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        delta_db_impl->db_options_.info_log,
        "Error reading delta during compaction/GC, key: %s (%s), status: %s",
        key.ToString(/* output_hex */ true).c_str(),
        delta_index.DebugString(/* output_hex */ true).c_str(),
        s.ToString().c_str());

    return false;
  }

  if (need_decompress && *compression_type != kNoCompression) {
    s = delta_db_impl->DecompressSlice(*delta, *compression_type, delta);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(
          delta_db_impl->db_options_.info_log,
          "Uncompression error during delta read from file: %" PRIu64
          " delta_offset: %" PRIu64 " delta_size: %" PRIu64
          " key: %s status: '%s'",
          delta_index.file_number(), delta_index.offset(), delta_index.size(),
          key.ToString(/* output_hex */ true).c_str(), s.ToString().c_str());

      return false;
    }
  }

  return true;
}

bool DeltaIndexCompactionFilterBase::WriteDeltaToNewFile(
    const Slice& key, const Slice& delta, uint64_t* new_delta_file_number,
    uint64_t* new_delta_offset) const {
  TEST_SYNC_POINT("DeltaIndexCompactionFilterBase::WriteDeltaToNewFile");
  assert(new_delta_file_number);
  assert(new_delta_offset);

  assert(delta_file_);
  *new_delta_file_number = delta_file_->DeltaFileNumber();

  assert(writer_);
  uint64_t new_key_offset = 0;
  const Status s = writer_->AddRecord(key, delta, kNoExpiration,
                                      &new_key_offset, new_delta_offset);

  if (!s.ok()) {
    const DeltaDBImpl* const delta_db_impl = context_.delta_db_impl;
    assert(delta_db_impl);

    ROCKS_LOG_ERROR(delta_db_impl->db_options_.info_log,
                    "Error writing delta to new file %s during compaction/GC, "
                    "key: %s, status: %s",
                    delta_file_->PathName().c_str(),
                    key.ToString(/* output_hex */ true).c_str(),
                    s.ToString().c_str());
    return false;
  }

  const uint64_t new_size =
      DeltaLogRecord::kHeaderSize + key.size() + delta.size();
  delta_file_->DeltaRecordAdded(new_size);

  DeltaDBImpl* const delta_db_impl = context_.delta_db_impl;
  assert(delta_db_impl);

  delta_db_impl->total_delta_size_ += new_size;

  return true;
}

bool DeltaIndexCompactionFilterBase::CloseAndRegisterNewDeltaFileIfNeeded()
    const {
  const DeltaDBImpl* const delta_db_impl = context_.delta_db_impl;
  assert(delta_db_impl);

  assert(delta_file_);
  if (delta_file_->GetFileSize() <
      delta_db_impl->bdb_options_.delta_file_size) {
    return true;
  }

  return CloseAndRegisterNewDeltaFile();
}

bool DeltaIndexCompactionFilterBase::CloseAndRegisterNewDeltaFile() const {
  DeltaDBImpl* const delta_db_impl = context_.delta_db_impl;
  assert(delta_db_impl);
  assert(delta_file_);

  Status s;

  {
    WriteLock wl(&delta_db_impl->mutex_);

    s = delta_db_impl->CloseDeltaFile(delta_file_);

    // Note: we delay registering the new delta file until it's closed to
    // prevent FIFO eviction from processing it during compaction/GC.
    delta_db_impl->RegisterDeltaFile(delta_file_);
  }

  assert(delta_file_->Immutable());

  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        delta_db_impl->db_options_.info_log,
        "Error closing new delta file %s during compaction/GC, status: %s",
        delta_file_->PathName().c_str(), s.ToString().c_str());
  }

  delta_file_.reset();
  return s.ok();
}

CompactionFilter::DeltaDecision
DeltaIndexCompactionFilterGC::PrepareDeltaOutput(const Slice& key,
                                                 const Slice& existing_value,
                                                 std::string* new_value) const {
  assert(new_value);

  const DeltaDBImpl* const delta_db_impl = context().delta_db_impl;
  (void)delta_db_impl;

  assert(delta_db_impl);
  assert(delta_db_impl->bdb_options_.enable_garbage_collection);

  DeltaIndex delta_index;
  const Status s = delta_index.DecodeFrom(existing_value);
  if (!s.ok()) {
    gc_stats_.SetError();
    return DeltaDecision::kCorruption;
  }

  if (delta_index.IsInlined()) {
    gc_stats_.AddDelta(delta_index.value().size());

    return DeltaDecision::kKeep;
  }

  gc_stats_.AddDelta(delta_index.size());

  if (delta_index.HasTTL()) {
    return DeltaDecision::kKeep;
  }

  if (delta_index.file_number() >= context_gc_.cutoff_file_number) {
    return DeltaDecision::kKeep;
  }

  // Note: each compaction generates its own delta files, which, depending on
  // the workload, might result in many small delta files. The total number of
  // files is bounded though (determined by the number of compactions and the
  // delta file size option).
  if (!OpenNewDeltaFileIfNeeded()) {
    gc_stats_.SetError();
    return DeltaDecision::kIOError;
  }

  PinnableSlice delta;
  CompressionType compression_type = kNoCompression;
  std::string compression_output;
  if (!ReadDeltaFromOldFile(key, delta_index, &delta, false,
                            &compression_type)) {
    gc_stats_.SetError();
    return DeltaDecision::kIOError;
  }

  // If the compression_type is changed, re-compress it with the new compression
  // type.
  if (compression_type != delta_db_impl->bdb_options_.compression) {
    if (compression_type != kNoCompression) {
      const Status status =
          delta_db_impl->DecompressSlice(delta, compression_type, &delta);
      if (!status.ok()) {
        gc_stats_.SetError();
        return DeltaDecision::kCorruption;
      }
    }
    if (delta_db_impl->bdb_options_.compression != kNoCompression) {
      delta_db_impl->GetCompressedSlice(delta, &compression_output);
      delta = PinnableSlice(&compression_output);
      delta.PinSelf();
    }
  }

  uint64_t new_delta_file_number = 0;
  uint64_t new_delta_offset = 0;
  if (!WriteDeltaToNewFile(key, delta, &new_delta_file_number,
                           &new_delta_offset)) {
    gc_stats_.SetError();
    return DeltaDecision::kIOError;
  }

  if (!CloseAndRegisterNewDeltaFileIfNeeded()) {
    gc_stats_.SetError();
    return DeltaDecision::kIOError;
  }

  DeltaIndex::EncodeDelta(new_value, new_delta_file_number, new_delta_offset,
                          delta.size(), compression_type);

  gc_stats_.AddRelocatedDelta(delta_index.size());

  return DeltaDecision::kChangeValue;
}

bool DeltaIndexCompactionFilterGC::OpenNewDeltaFileIfNeeded() const {
  if (IsDeltaFileOpened()) {
    return true;
  }
  bool result = DeltaIndexCompactionFilterBase::OpenNewDeltaFileIfNeeded();
  if (result) {
    gc_stats_.AddNewFile();
  }
  return result;
}

std::unique_ptr<CompactionFilter>
DeltaIndexCompactionFilterFactoryBase::CreateUserCompactionFilterFromFactory(
    const CompactionFilter::Context& context) const {
  std::unique_ptr<CompactionFilter> user_comp_filter_from_factory;
  if (user_comp_filter_factory_) {
    user_comp_filter_from_factory =
        user_comp_filter_factory_->CreateCompactionFilter(context);
  }
  return user_comp_filter_from_factory;
}

std::unique_ptr<CompactionFilter>
DeltaIndexCompactionFilterFactory::CreateCompactionFilter(
    const CompactionFilter::Context& _context) {
  assert(clock());

  int64_t current_time = 0;
  Status s = clock()->GetCurrentTime(&current_time);
  if (!s.ok()) {
    return nullptr;
  }
  assert(current_time >= 0);

  assert(delta_db_impl());

  DeltaCompactionContext context;
  delta_db_impl()->GetCompactionContext(&context);

  std::unique_ptr<CompactionFilter> user_comp_filter_from_factory =
      CreateUserCompactionFilterFromFactory(_context);

  return std::unique_ptr<CompactionFilter>(new DeltaIndexCompactionFilter(
      std::move(context), user_comp_filter(),
      std::move(user_comp_filter_from_factory), current_time, statistics()));
}

std::unique_ptr<CompactionFilter>
DeltaIndexCompactionFilterFactoryGC::CreateCompactionFilter(
    const CompactionFilter::Context& _context) {
  assert(clock());

  int64_t current_time = 0;
  Status s = clock()->GetCurrentTime(&current_time);
  if (!s.ok()) {
    return nullptr;
  }
  assert(current_time >= 0);

  assert(delta_db_impl());

  DeltaCompactionContext context;
  DeltaCompactionContextGC context_gc;
  delta_db_impl()->GetCompactionContext(&context, &context_gc);

  std::unique_ptr<CompactionFilter> user_comp_filter_from_factory =
      CreateUserCompactionFilterFromFactory(_context);

  return std::unique_ptr<CompactionFilter>(new DeltaIndexCompactionFilterGC(
      std::move(context), std::move(context_gc), user_comp_filter(),
      std::move(user_comp_filter_from_factory), current_time, statistics()));
}

}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
