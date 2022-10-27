//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <ostream>
#include <sstream>

#include "db/delta/delta_file_addition.h"
#include "logging/event_logger.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

// Tags for custom fields. Note that these get persisted in the manifest,
// so existing tags should not be modified.
enum DeltaFileAddition::CustomFieldTags : uint32_t {
  kEndMarker,

  // Add forward compatible fields here

  /////////////////////////////////////////////////////////////////////

  kForwardIncompatibleMask = 1 << 6,

  // Add forward incompatible fields here
};

void DeltaFileAddition::EncodeTo(std::string* output) const {
  PutVarint64(output, delta_file_number_);
  PutVarint64(output, total_delta_count_);
  PutVarint64(output, total_delta_bytes_);
  PutLengthPrefixedSlice(output, checksum_method_);
  PutLengthPrefixedSlice(output, checksum_value_);

  // Encode any custom fields here. The format to use is a Varint32 tag (see
  // CustomFieldTags above) followed by a length prefixed slice. Unknown custom
  // fields will be ignored during decoding unless they're in the forward
  // incompatible range.

  TEST_SYNC_POINT_CALLBACK("DeltaFileAddition::EncodeTo::CustomFields", output);

  PutVarint32(output, kEndMarker);
}

Status DeltaFileAddition::DecodeFrom(Slice* input) {
  constexpr char class_name[] = "DeltaFileAddition";

  if (!GetVarint64(input, &delta_file_number_)) {
    return Status::Corruption(class_name, "Error decoding delta file number");
  }

  if (!GetVarint64(input, &total_delta_count_)) {
    return Status::Corruption(class_name, "Error decoding total delta count");
  }

  if (!GetVarint64(input, &total_delta_bytes_)) {
    return Status::Corruption(class_name, "Error decoding total delta bytes");
  }

  Slice checksum_method;
  if (!GetLengthPrefixedSlice(input, &checksum_method)) {
    return Status::Corruption(class_name, "Error decoding checksum method");
  }
  checksum_method_ = checksum_method.ToString();

  Slice checksum_value;
  if (!GetLengthPrefixedSlice(input, &checksum_value)) {
    return Status::Corruption(class_name, "Error decoding checksum value");
  }
  checksum_value_ = checksum_value.ToString();

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

std::string DeltaFileAddition::DebugString() const {
  std::ostringstream oss;

  oss << *this;

  return oss.str();
}

std::string DeltaFileAddition::DebugJSON() const {
  JSONWriter jw;

  jw << *this;

  jw.EndObject();

  return jw.Get();
}

bool operator==(const DeltaFileAddition& lhs, const DeltaFileAddition& rhs) {
  return lhs.GetDeltaFileNumber() == rhs.GetDeltaFileNumber() &&
         lhs.GetTotalDeltaCount() == rhs.GetTotalDeltaCount() &&
         lhs.GetTotalDeltaBytes() == rhs.GetTotalDeltaBytes() &&
         lhs.GetChecksumMethod() == rhs.GetChecksumMethod() &&
         lhs.GetChecksumValue() == rhs.GetChecksumValue();
}

bool operator!=(const DeltaFileAddition& lhs, const DeltaFileAddition& rhs) {
  return !(lhs == rhs);
}

std::ostream& operator<<(std::ostream& os,
                         const DeltaFileAddition& delta_file_addition) {
  os << "delta_file_number: " << delta_file_addition.GetDeltaFileNumber()
     << " total_delta_count: " << delta_file_addition.GetTotalDeltaCount()
     << " total_delta_bytes: " << delta_file_addition.GetTotalDeltaBytes()
     << " checksum_method: " << delta_file_addition.GetChecksumMethod()
     << " checksum_value: "
     << Slice(delta_file_addition.GetChecksumValue()).ToString(/* hex */ true);

  return os;
}

JSONWriter& operator<<(JSONWriter& jw,
                       const DeltaFileAddition& delta_file_addition) {
  jw << "DeltaFileNumber" << delta_file_addition.GetDeltaFileNumber()
     << "TotalDeltaCount" << delta_file_addition.GetTotalDeltaCount()
     << "TotalDeltaBytes" << delta_file_addition.GetTotalDeltaBytes()
     << "ChecksumMethod" << delta_file_addition.GetChecksumMethod()
     << "ChecksumValue"
     << Slice(delta_file_addition.GetChecksumValue()).ToString(/* hex */ true);

  return jw;
}

}  // namespace ROCKSDB_NAMESPACE
