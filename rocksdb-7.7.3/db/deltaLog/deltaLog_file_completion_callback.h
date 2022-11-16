//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class DeltaLogFileCompletionCallback {
 public:
  DeltaLogFileCompletionCallback(
      InstrumentedMutex* mutex, ErrorHandler* error_handler,
      EventLogger* event_logger,
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const std::string& dbname)
      : event_logger_(event_logger), listeners_(listeners), dbname_(dbname) {
#ifndef ROCKSDB_LITE
    mutex_ = mutex;
    error_handler_ = error_handler;
#else
    (void)mutex;
    (void)error_handler;
#endif  // ROCKSDB_LITE
  }

  void OnDeltaLogFileCreationStarted(
      const std::string& file_name, const std::string& column_family_name,
      int job_id, DeltaLogFileCreationReason creation_reason) {
#ifndef ROCKSDB_LITE
    // Notify the listeners.
    EventHelpers::NotifyDeltaLogFileCreationStarted(
        listeners_, dbname_, column_family_name, file_name, job_id,
        creation_reason);
#else
    (void)file_name;
    (void)column_family_name;
    (void)job_id;
    (void)creation_reason;
#endif
  }

  Status OnDeltaLogFileCompleted(const std::string& file_name,
                                 const std::string& column_family_name,
                                 int job_id, uint64_t file_id,
                                 DeltaLogFileCreationReason creation_reason,
                                 const Status& report_status,
                                 uint64_t deltaLog_count,
                                 uint64_t deltaLog_bytes) {
    Status s;

    // Notify the listeners.
    EventHelpers::LogAndNotifyDeltaLogFileCreationFinished(
        event_logger_, listeners_, dbname_, column_family_name, file_name,
        job_id, file_id, creation_reason,
        (!report_status.ok() ? report_status : s), deltaLog_count,
        deltaLog_bytes);
    return s;
  }

 private:
#ifndef ROCKSDB_LITE
  InstrumentedMutex* mutex_;
  ErrorHandler* error_handler_;
#endif  // ROCKSDB_LITE
  EventLogger* event_logger_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::string dbname_;
};
}  // namespace ROCKSDB_NAMESPACE
