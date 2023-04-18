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
    kAlignLinuxIO = 2, 
    kPreadWrite = 3};

typedef struct FileOpStatus {
    bool success_;
    uint64_t physicalSize_;
    uint64_t logicalSize_;
    uint64_t bufferedSize_;
    FileOpStatus(bool success,
        uint64_t physicalSize,
        uint64_t logicalSize,
        uint64_t bufferedSize)
    {
        success_ = success;
        physicalSize_ = physicalSize;
        logicalSize_ = logicalSize;
        bufferedSize_ = bufferedSize;
    };
    FileOpStatus() {};
} FileOpStatus;

class FileOperation {
public:
    FileOperation(fileOperationType operationType);
    FileOperation(fileOperationType operationType, uint64_t fileSize, uint64_t bufferSize);
    ~FileOperation();
    FileOpStatus writeFile(char* write_buf, uint64_t size);
    FileOpStatus writeAndFlushFile(char* write_buf, uint64_t size);
    FileOpStatus readFile(char* read_buf, uint64_t size);
    FileOpStatus positionedReadFile(char* read_buf, uint64_t offset, uint64_t size);
    FileOpStatus flushFile();

    bool openFile(string path);
    bool createFile(string path);
    bool createThenOpenFile(string path);
    bool closeFile();
    bool isFileOpen();
    uint64_t getFileSize();
    uint64_t getCachedFileSize();
    uint64_t getFilePhysicalSize(string path);
    uint64_t getFileBufferedSize();
    void markDirectDataAddress(uint64_t data);

private:
    fileOperationType operationType_;
    fstream fileStream_;
    int fd_;
    uint64_t page_size_ = sysconf(_SC_PAGESIZE);
    uint64_t page_size_m4_ = sysconf(_SC_PAGESIZE) - sizeof(uint32_t);
    uint64_t disk_size_ = 0;
    uint64_t data_size_ = 0;
    uint64_t newlyCreatedFileFlag_ = false;
    uint64_t preAllocateFileSize_ = 0;
    char* globalWriteBuffer_ = nullptr;
    uint64_t buf_used_size_ = 0;
    uint64_t buf_size_ = 0;

    uint64_t mark_data_ = 0;
    uint64_t mark_disk_ = 0;
};

} // namespace DELTAKV_NAMESPACE
