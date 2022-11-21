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
