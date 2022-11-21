#pragma once

#include <bits/stdc++.h>

using namespace std;

typedef struct externalValueType {
    bool mergeFlag_;
};

typedef struct externalIndexInfo {
    uint32_t externalFileID_;
    uint32_t externalFileOffset_;
    uint32_t externalContentSize_;
};
