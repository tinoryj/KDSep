#pragma once

#include "rocksdb/options.h"
#include "utils/loggerColor.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

enum fileOperationType { kFstream = 0,
    kDirectIO = 1 };
enum fileOperationSetPointerOps { kBegin = 0,
    kEnd = 1 };

class FileOperation {
public:
    FileOperation(fileOperationType operationType);
    ~FileOperation();
    bool write(char* contentBuffer, uint64_t contentSize);
    bool read(char* contentBuffer, uint64_t contentSize);
    bool flush();
    bool open(string path);
    bool create(string path);
    bool close();
    bool resetPointer(fileOperationSetPointerOps ops, uint64_t offset);
    uint64_t getFileSize();

private:
    fileOperationType operationType_;
    fstream fileStream_;
    int fileDirect_;
};

} // namespace DELTAKV_NAMESPACE