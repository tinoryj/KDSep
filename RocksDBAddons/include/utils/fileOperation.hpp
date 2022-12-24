#pragma once

#include "rocksdb/options.h"
#include "utils/debug.hpp"
#include <bits/stdc++.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

enum fileOperationType { kFstream = 0,
    kDirectIO = 1,
    kAlignLinuxIO = 2 };

typedef struct fileOperationStatus_t {
    bool success_;
    uint64_t physicalSize_;
    uint64_t logicalSize_;
    uint64_t bufferedSize_;
    fileOperationStatus_t(bool success,
        uint64_t physicalSize,
        uint64_t logicalSize,
        uint64_t bufferedSize)
    {
        success_ = success;
        physicalSize_ = physicalSize;
        logicalSize_ = logicalSize;
        bufferedSize_ = bufferedSize;
    };
    fileOperationStatus_t() {};
} fileOperationStatus_t;

class FileOperation {
public:
    FileOperation(fileOperationType operationType);
    FileOperation(fileOperationType operationType, uint64_t fileSize, uint64_t bufferSize);
    ~FileOperation();
    fileOperationStatus_t writeFile(char* contentBuffer, uint64_t contentSize);
    fileOperationStatus_t readFile(char* contentBuffer, uint64_t contentSize);
    fileOperationStatus_t flushFile();

    bool openFile(string path);
    bool createFile(string path);
    bool createThenOpenFile(string path);
    bool closeFile();
    bool isFileOpen();
    uint64_t getFileSize();
    uint64_t getFilePhysicalSize(string path);
    uint64_t getFileBufferedSize();

private:
    fileOperationType operationType_;
    fstream fileStream_;
    int fileDirect_;
    uint64_t directIOPageSize_ = sysconf(_SC_PAGESIZE);
    uint64_t directIOWriteFileSize_ = 0;
    uint64_t directIOActualWriteFileSize_ = 0;
    uint64_t newlyCreatedFileFlag_ = false;
    uint64_t preAllocateFileSize_ = 0;
    char* globalWriteBuffer_ = nullptr;
    int bufferUsedSize_ = 0;
    uint64_t globalBufferSize_ = 0;
};

} // namespace DELTAKV_NAMESPACE
