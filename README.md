  compaction_job_info->delta_compression_type =
      c->mutable_cf_options()->delta_compression_type;

  // Update DeltaFilesInfo.
  for (const auto& delta_file : c->edit()->GetDeltaFileAdditions()) {
    DeltaFileAdditionInfo delta_file_addition_info(
        DeltaFileName(c->immutable_options()->cf_paths.front().path,
                     delta_file.GetDeltaFileNumber()) /*delta_file_path*/,
        delta_file.GetDeltaFileNumber(), delta_file.GetTotalDeltaCount(),
        delta_file.GetTotalDeltaBytes());
    compaction_job_info->delta_file_addition_infos.emplace_back(
        std::move(delta_file_addition_info));
  }

  // Update DeltaFilesGarbageInfo.
  for (const auto& delta_file : c->edit()->GetDeltaFileGarbages()) {
    DeltaFileGarbageInfo delta_file_garbage_info(
        DeltaFileName(c->immutable_options()->cf_paths.front().path,
                     delta_file.GetDeltaFileNumber()) /*delta_file_path*/,
        delta_file.GetDeltaFileNumber(), delta_file.GetGarbageDeltaCount(),
        delta_file.GetGarbageDeltaBytes());
    compaction_job_info->delta_file_garbage_infos.emplace_back(
        std::move(delta_file_garbage_info));
  }