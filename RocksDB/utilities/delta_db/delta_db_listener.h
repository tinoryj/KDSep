//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <atomic>

#include "rocksdb/listener.h"
#include "util/mutexlock.h"
#include "utilities/delta_db/delta_db_impl.h"

namespace ROCKSDB_NAMESPACE {
namespace delta_db {

class DeltaDBListener : public EventListener {
 public:
  explicit DeltaDBListener(DeltaDBImpl* delta_db_impl)
      : delta_db_impl_(delta_db_impl) {}

  void OnFlushBegin(DB* /*db*/, const FlushJobInfo& /*info*/) override {
    assert(delta_db_impl_ != nullptr);
    delta_db_impl_->SyncDeltaFiles();
  }

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& /*info*/) override {
    assert(delta_db_impl_ != nullptr);
    delta_db_impl_->UpdateLiveSSTSize();
  }

  void OnCompactionCompleted(DB* /*db*/,
                             const CompactionJobInfo& /*info*/) override {
    assert(delta_db_impl_ != nullptr);
    delta_db_impl_->UpdateLiveSSTSize();
  }

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "DeltaDBListener"; }

 protected:
  DeltaDBImpl* delta_db_impl_;
};

class DeltaDBListenerGC : public DeltaDBListener {
 public:
  explicit DeltaDBListenerGC(DeltaDBImpl* delta_db_impl)
      : DeltaDBListener(delta_db_impl) {}

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "DeltaDBListenerGC"; }
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    DeltaDBListener::OnFlushCompleted(db, info);

    assert(delta_db_impl_);
    delta_db_impl_->ProcessFlushJobInfo(info);
  }

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override {
    DeltaDBListener::OnCompactionCompleted(db, info);

    assert(delta_db_impl_);
    delta_db_impl_->ProcessCompactionJobInfo(info);
  }
};

}  // namespace delta_db
}  // namespace ROCKSDB_NAMESPACE
#endif  // !ROCKSDB_LITE
