
bool CompactionIterator::ExtractLargeValueIfNeededImpl() {
  if (!deltaLog_file_builder_) {
    return false;
  }

  deltaLog_index_.clear();
  const Status s = deltaLog_file_builder_->Add(user_key(), value_, &deltaLog_index_);

  if (!s.ok()) {
    status_ = s;
    validity_info_.Invalidate();

    return false;
  }

  if (deltaLog_index_.empty()) {
    return false;
  }

  value_ = deltaLog_index_;

  return true;
}