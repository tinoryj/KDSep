//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include <unordered_set>

#include "db/delta/delta_index.h"
#include "monitoring/statistics.h"
#include "rocksdb/compaction_filter.h"
#include "utilities/compaction_filters/layered_compaction_filter_base.h"
#include "utilities/delta_db/delta_db_gc_stats.h"
#include "utilities/delta_db/delta_db_impl.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;
namespace delta_db {

struct DeltaCompactionContext {
  DeltaDBImpl* delta_db_impl = nullptr;
  uint64_t next_file_number = 0;
  std::unordered_set<uint64_t> current_delta_files;
  SequenceNumber fifo_eviction_seq = 0;
  uint64_t evict_expiration_up_to = 0;
};

struct DeltaCompactionContextGC {
  uint64_t cutoff_file_number = 0;
};

// Compaction filter that deletes expired delta indexes from the base DB.
// Comes into two varieties, one for the non-GC case and one for the GC case.
class DeltaIndexCompactionFilterBase : public LayeredCompactionFilterBase {
 public:
  DeltaIndexCompactionFilterBase(
      DeltaCompactionContext&& _context,
      const CompactionFilter* _user_comp_filter,
      std::unique_ptr<const CompactionFilter> _user_comp_filter_from_factory,
      uint64_t current_time, Statistics* stats)
      : LayeredCompactionFilterBase(_user_comp_filter,
                                    std::move(_user_comp_filter_from_factory)),
        context_(std::move(_context)),
        current_time_(current_time),
        statistics_(stats) {}

  ~DeltaIndexCompactionFilterBase() override;

  // Filter expired delta indexes regardless of snapshots.
  bool IgnoreSnapshots() const override { return true; }

  Decision FilterV2(int level, const Slice& key, ValueType value_type,
                    const Slice& value, std::string* new_value,
                    std::string* skip_until) const override;

  bool IsStackedDeltaDbInternalCompactionFilter() const override {
    return true;
  }

 protected:
  bool IsDeltaFileOpened() const;
  virtual bool OpenNewDeltaFileIfNeeded() const;
  bool ReadDeltaFromOldFile(const Slice& key, const DeltaIndex& delta_index,
                            PinnableSlice* delta, bool need_decompress,
                            CompressionType* compression_type) const;
  bool WriteDeltaToNewFile(const Slice& key, const Slice& delta,
                           uint64_t* new_delta_file_number,
                           uint64_t* new_delta_offset) const;
  bool CloseAndRegisterNewDeltaFileIfNeeded() const;
  bool CloseAndRegisterNewDeltaFile() const;

  Statistics* statistics() const { return statistics_; }
  const DeltaCompactionContext& context() const { return context_; }

 private:
  Decision HandleValueChange(const Slice& key, std::string* new_value) const;

 private:
  DeltaCompactionContext context_;
  const uint64_t current_time_;
  Statistics* statistics_;

  mutable std::shared_ptr<DeltaFile> delta_file_;
  mutable std::shared_ptr<DeltaLogWriter> writer_;

  // It is safe to not using std::atomic since the compaction filter, created
  // from a compaction filter factroy, will not be called from multiple threads.
  mutable uint64_t expired_count_ = 0;
  mutable uint64_t expired_size_ = 0;
  mutable uint64_t evicted_count_ = 0;
  mutable uint64_t evicted_size_ = 0;
};

class DeltaIndexCompactionFilter : public DeltaIndexCompactionFilterBase {
 public:
  DeltaIndexCompactionFilter(
      DeltaCompactionContext&& _context,
      const CompactionFilter* _user_comp_filter,
      std::unique_ptr<const CompactionFilter> _user_comp_filter_from_factory,
      uint64_t current_time, Statistics* stats)
      : DeltaIndexCompactionFilterBase(
            std::move(_context), _user_comp_filter,
            std::move(_user_comp_filter_from_factory), current_time, stats) {}

  const char* Name() const override { return "DeltaIndexCompactionFilter"; }
};

class DeltaIndexCompactionFilterGC : public DeltaIndexCompactionFilterBase {
 public:
  DeltaIndexCompactionFilterGC(
      DeltaCompactionContext&& _context, DeltaCompactionContextGC&& context_gc,
      const CompactionFilter* _user_comp_filter,
      std::unique_ptr<const CompactionFilter> _user_comp_filter_from_factory,
      uint64_t current_time, Statistics* stats)
      : DeltaIndexCompactionFilterBase(
            std::move(_context), _user_comp_filter,
            std::move(_user_comp_filter_from_factory), current_time, stats),
        context_gc_(std::move(context_gc)) {}

  ~DeltaIndexCompactionFilterGC() override;

  const char* Name() const override { return "DeltaIndexCompactionFilterGC"; }

  DeltaDecision PrepareDeltaOutput(const Slice& key,
                                   const Slice& existing_value,
                                   std::string* new_value) const override;

 private:
  bool OpenNewDeltaFileIfNeeded() const override;

 private:
  DeltaCompactionContextGC context_gc_;
  mutable DeltaDBGarbageCollectionStats gc_stats_;
};

// Compaction filter factory; similarly to the filters above, it comes
// in two flavors, one that creates filters that support GC, and one
// that creates non-GC filters.
class DeltaIndexCompactionFilterFactoryBase : public CompactionFilterFactory {
 public:
  DeltaIndexCompactionFilterFactoryBase(DeltaDBImpl* _delta_db_impl,
                                        SystemClock* _clock,
                                        const ColumnFamilyOptions& _cf_options,
                                        Statistics* _statistics)
      : delta_db_impl_(_delta_db_impl),
        clock_(_clock),
        statistics_(_statistics),
        user_comp_filter_(_cf_options.compaction_filter),
        user_comp_filter_factory_(_cf_options.compaction_filter_factory) {}

 protected:
  std::unique_ptr<CompactionFilter> CreateUserCompactionFilterFromFactory(
      const CompactionFilter::Context& context) const;

  DeltaDBImpl* delta_db_impl() const { return delta_db_impl_; }
  SystemClock* clock() const { return clock_; }
  Statistics* statistics() const { return statistics_; }
  const CompactionFilter* user_comp_filter() const { return user_comp_filter_; }

 private:
  DeltaDBImpl* delta_db_impl_;
  SystemClock* clock_;
  Statistics* statistics_;
  const CompactionFilter* user_comp_filter_;
  std::shared_ptr<CompactionFilterFactory> user_comp_filter_factory_;
};

class DeltaIndexCompactionFilterFactory
    : public DeltaIndexCompactionFilterFactoryBase {
 public:
  DeltaIndexCompactionFilterFactory(DeltaDBImpl* _delta_db_impl,
                                    SystemClock* _clock,
                                    const ColumnFamilyOptions& _cf_options,
                                    Statistics* _statistics)
      : DeltaIndexCompactionFilterFactoryBase(_delta_db_impl, _clock,
                                              _cf_options, _statistics) {}

  const char* Name() const override {
    return "DeltaIndexCompactionFilterFactory";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override;
};

class DeltaIndexCompactionFilterFactoryGC
    : public DeltaIndexCompactionFilterFactoryBase {
 public:
  DeltaIndexCompactionFilterFactoryGC(DeltaDBImpl* _delta_db_impl,
                                      SystemClock* _clock,
                                      const ColumnFamilyOptions& _cf_options,
                                      Statistics* _statistics)
      : DeltaIndexCompactionFilterFactoryBase(_delta_db_impl, _clock,
                                              _cf_options, _statistics) {}

  const char* Name() const override {
    return "DeltaIndexCompactionFilterFactoryGC";
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override;
};

}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
