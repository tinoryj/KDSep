//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/deltaLog/deltaLog_file_addition.h"

#include <ostream>
#include <sstream>

#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Tags for custom fields. Note that these get persisted in the manifest,
// so existing tags should not be modified.
enum DeltaLogFileAddition::CustomFieldTags : uint32_t {
  kEndMarker,

  // Add forward compatible fields here

  /////////////////////////////////////////////////////////////////////

  kForwardIncompatibleMask = 1 << 6,

  // Add forward incompatible fields here
};

void DeltaLogFileAddition::EncodeTo(std::string* output) const {
  PutVarint64(output, deltaLog_file_id_);
  PutVarint64(output, total_deltaLog_count_);
  PutVarint64(output, total_deltaLog_bytes_);
  // Encode any custom fields here. The format to use is a Varint32 tag (see
  // CustomFieldTags above) followed by a length prefixed slice. Unknown custom
  // fields will be ignored during decoding unless they're in the forward
  // incompatible range.

  TEST_SYNC_POINT_CALLBACK("DeltaLogFileAddition::EncodeTo::CustomFields",
                           output);

  PutVarint32(output, kEndMarker);
}

Status DeltaLogFileAddition::DecodeFrom(Slice* input) {
  constexpr char class_name[] = "DeltaLogFileAddition";

  if (!GetVarint64(input, &deltaLog_file_id_)) {
    return Status::Corruption(class_name,
                              "Error decoding deltaLog file number");
  }

  if (!GetVarint64(input, &total_deltaLog_count_)) {
    return Status::Corruption(class_name,
                              "Error decoding total deltaLog count");
  }

  if (!GetVarint64(input, &total_deltaLog_bytes_)) {
    return Status::Corruption(class_name,
                              "Error decoding total deltaLog bytes");
  }

  while (true) {
    uint32_t custom_field_tag = 0;
    if (!GetVarint32(input, &custom_field_tag)) {
      return Status::Corruption(class_name, "Error decoding custom field tag");
    }

    if (custom_field_tag == kEndMarker) {
      break;
    }

    if (custom_field_tag & kForwardIncompatibleMask) {
      return Status::Corruption(
          class_name, "Forward incompatible custom field encountered");
    }

    Slice custom_field_value;
    if (!GetLengthPrefixedSlice(input, &custom_field_value)) {
      return Status::Corruption(class_name,
                                "Error decoding custom field value");
    }
  }

  return Status::OK();
}

std::string DeltaLogFileAddition::DebugString() const {
  std::ostringstream oss;

  oss << *this;

  return oss.str();
}

std::string DeltaLogFileAddition::DebugJSON() const {
  JSONWriter jw;

  jw << *this;

  jw.EndObject();

  return jw.Get();
}

bool operator==(const DeltaLogFileAddition& lhs,
                const DeltaLogFileAddition& rhs) {
  return lhs.GetDeltaLogFileID() == rhs.GetDeltaLogFileID() &&
         lhs.GetTotalDeltaLogCount() == rhs.GetTotalDeltaLogCount() &&
         lhs.GetTotalDeltaLogBytes() == rhs.GetTotalDeltaLogBytes();
}

bool operator!=(const DeltaLogFileAddition& lhs,
                const DeltaLogFileAddition& rhs) {
  return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& os,
                         const DeltaLogFileAddition& deltaLog_file_addition) {
  os << "deltaLog_file_id: " << deltaLog_file_addition.GetDeltaLogFileID()
     << " total_deltaLog_count: "
     << deltaLog_file_addition.GetTotalDeltaLogCount()
     << " total_deltaLog_bytes: "
     << deltaLog_file_addition.GetTotalDeltaLogBytes();

  return os;
}

JSONWriter& operator<<(JSONWriter& jw,
                       const DeltaLogFileAddition& deltaLog_file_addition) {
  jw << "DeltaLogFileNumber" << deltaLog_file_addition.GetDeltaLogFileID()
     << "TotalDeltaLogCount" << deltaLog_file_addition.GetTotalDeltaLogCount()
     << "TotalDeltaLogBytes" << deltaLog_file_addition.GetTotalDeltaLogBytes();

  return jw;
}

}  // namespace ROCKSDB_NAMESPACE
