//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <ostream>
#include <sstream>

#include "db/delta/delta_file_garbage.h"
#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Tags for custom fields. Note that these get persisted in the manifest,
// so existing tags should not be modified.
enum DeltaFileGarbage::CustomFieldTags : uint32_t {
  kEndMarker,

  // Add forward compatible fields here

  /////////////////////////////////////////////////////////////////////

  kForwardIncompatibleMask = 1 << 6,

  // Add forward incompatible fields here
};

void DeltaFileGarbage::EncodeTo(std::string* output) const {
  PutVarint64(output, delta_file_number_);
  PutVarint64(output, garbage_delta_count_);
  PutVarint64(output, garbage_delta_bytes_);

  // Encode any custom fields here. The format to use is a Varint32 tag (see
  // CustomFieldTags above) followed by a length prefixed slice. Unknown custom
  // fields will be ignored during decoding unless they're in the forward
  // incompatible range.

  TEST_SYNC_POINT_CALLBACK("DeltaFileGarbage::EncodeTo::CustomFields", output);

  PutVarint32(output, kEndMarker);
}

Status DeltaFileGarbage::DecodeFrom(Slice* input) {
  constexpr char class_name[] = "DeltaFileGarbage";

  if (!GetVarint64(input, &delta_file_number_)) {
    return Status::Corruption(class_name, "Error decoding delta file number");
  }

  if (!GetVarint64(input, &garbage_delta_count_)) {
    return Status::Corruption(class_name, "Error decoding garbage delta count");
  }

  if (!GetVarint64(input, &garbage_delta_bytes_)) {
    return Status::Corruption(class_name, "Error decoding garbage delta bytes");
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

std::string DeltaFileGarbage::DebugString() const {
  std::ostringstream oss;

  oss << *this;

  return oss.str();
}

std::string DeltaFileGarbage::DebugJSON() const {
  JSONWriter jw;

  jw << *this;

  jw.EndObject();

  return jw.Get();
}

bool operator==(const DeltaFileGarbage& lhs, const DeltaFileGarbage& rhs) {
  return lhs.GetDeltaFileNumber() == rhs.GetDeltaFileNumber() &&
         lhs.GetGarbageDeltaCount() == rhs.GetGarbageDeltaCount() &&
         lhs.GetGarbageDeltaBytes() == rhs.GetGarbageDeltaBytes();
}

bool operator!=(const DeltaFileGarbage& lhs, const DeltaFileGarbage& rhs) {
  return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& os,
                         const DeltaFileGarbage& delta_file_garbage) {
  os << "delta_file_number: " << delta_file_garbage.GetDeltaFileNumber()
     << " garbage_delta_count: " << delta_file_garbage.GetGarbageDeltaCount()
     << " garbage_delta_bytes: " << delta_file_garbage.GetGarbageDeltaBytes();

  return os;
}

JSONWriter& operator<<(JSONWriter& jw,
                       const DeltaFileGarbage& delta_file_garbage) {
  jw << "DeltaFileNumber" << delta_file_garbage.GetDeltaFileNumber()
     << "GarbageDeltaCount" << delta_file_garbage.GetGarbageDeltaCount()
     << "GarbageDeltaBytes" << delta_file_garbage.GetGarbageDeltaBytes();

  return jw;
}

}  // namespace ROCKSDB_NAMESPACE
