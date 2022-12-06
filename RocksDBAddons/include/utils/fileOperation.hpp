#pragma once

#include "rocksdb/options.h"
#include "utils/debug.hpp"
#include <bits/stdc++.h>
#include <fcntl.h>
#include <sys/stat.h>

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
    bool writeFile(char* contentBuffer, uint64_t contentSize);
    bool readFile(char* contentBuffer, uint64_t contentSize);
    bool flushFile();
    bool openFile(string path);
    bool createFile(string path);
    bool closeFile();
    bool resetPointer(fileOperationSetPointerOps ops);
    uint64_t getFileSize();
    uint64_t getFilePhysicalSize(string path);

private:
    fileOperationType operationType_;
    fstream fileStream_;
    int fileDirect_;
    uint64_t directIOPageSize_ = sysconf(_SC_PAGESIZE);
    uint64_t directIOWriteFileSize_ = 0;
    uint64_t newlyCreatedFileFlag_ = false;
};

} // namespace DELTAKV_NAMESPACE