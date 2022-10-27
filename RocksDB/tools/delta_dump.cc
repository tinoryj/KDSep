//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include <getopt.h>

#include <cstdio>
#include <string>
#include <unordered_map>

#include "utilities/delta_db/delta_dump_tool.h"

using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::delta_db::DeltaDumpTool;

int main(int argc, char** argv) {
  using DisplayType = DeltaDumpTool::DisplayType;
  const std::unordered_map<std::string, DisplayType> display_types = {
      {"none", DisplayType::kNone},
      {"raw", DisplayType::kRaw},
      {"hex", DisplayType::kHex},
      {"detail", DisplayType::kDetail},
  };
  const struct option options[] = {
      {"help", no_argument, nullptr, 'h'},
      {"file", required_argument, nullptr, 'f'},
      {"show_key", optional_argument, nullptr, 'k'},
      {"show_delta", optional_argument, nullptr, 'b'},
      {"show_uncompressed_delta", optional_argument, nullptr, 'r'},
      {"show_summary", optional_argument, nullptr, 's'},
  };
  DisplayType show_key = DisplayType::kRaw;
  DisplayType show_delta = DisplayType::kNone;
  DisplayType show_uncompressed_delta = DisplayType::kNone;
  bool show_summary = false;
  std::string file;
  while (true) {
    int c = getopt_long(argc, argv, "hk::b::f:", options, nullptr);
    if (c < 0) {
      break;
    }
    std::string arg_str(optarg ? optarg : "");
    switch (c) {
      case 'h':
        fprintf(stdout,
                "Usage: delta_dump --file=filename "
                "[--show_key[=none|raw|hex|detail]] "
                "[--show_delta[=none|raw|hex|detail]] "
                "[--show_uncompressed_delta[=none|raw|hex|detail]] "
                "[--show_summary]\n");
        return 0;
      case 'f':
        file = optarg;
        break;
      case 'k':
        if (optarg) {
          if (display_types.count(arg_str) == 0) {
            fprintf(stderr, "Unrecognized key display type.\n");
            return -1;
          }
          show_key = display_types.at(arg_str);
        }
        break;
      case 'b':
        if (optarg) {
          if (display_types.count(arg_str) == 0) {
            fprintf(stderr, "Unrecognized delta display type.\n");
            return -1;
          }
          show_delta = display_types.at(arg_str);
        } else {
          show_delta = DisplayType::kHex;
        }
        break;
      case 'r':
        if (optarg) {
          if (display_types.count(arg_str) == 0) {
            fprintf(stderr, "Unrecognized delta display type.\n");
            return -1;
          }
          show_uncompressed_delta = display_types.at(arg_str);
        } else {
          show_uncompressed_delta = DisplayType::kHex;
        }
        break;
      case 's':
        show_summary = true;
        break;
      default:
        fprintf(stderr, "Unrecognized option.\n");
        return -1;
    }
  }
  DeltaDumpTool tool;
  Status s = tool.Run(file, show_key, show_delta, show_uncompressed_delta,
                      show_summary);
  if (!s.ok()) {
    fprintf(stderr, "Failed: %s\n", s.ToString().c_str());
    return -1;
  }
  return 0;
}
#else
#include <stdio.h>
int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "Not supported in lite mode.\n");
  return -1;
}
#endif  // ROCKSDB_LITE
