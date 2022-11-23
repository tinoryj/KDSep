#pragma once

#include <bits/stdc++.h>

using namespace std;

typedef struct externalValueType {
    bool mergeFlag_;
} externalValueType;

typedef struct externalIndexInfo {
    uint32_t externalFileID_;
    uint32_t externalFileOffset_;
    uint32_t externalContentSize_;
} externalIndexInfo;

typedef struct hashStoreFileMetaData {
    uint64_t deltaLog_file_id_;
    uint64_t total_deltaLog_count_;
    uint64_t total_deltaLog_bytes_;
} hashStoreFileMetaData;
