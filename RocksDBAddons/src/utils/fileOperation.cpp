#include "utils/fileOperation.hpp"

using namespace std;

namespace DELTAKV_NAMESPACE {

FileOperation::FileOperation(fileOperationType operationType)
{
    operationType_ = operationType;
    fileDirect_ = -1;
    preAllocateFileSize_ = 256 * 1024;
    globalWriteBuffer_ = new char[directIOPageSize_ - sizeof(uint32_t)];
    globalBufferSize_ = directIOPageSize_ - sizeof(uint32_t);
    bufferUsedSize_ = 0;
}

FileOperation::FileOperation(fileOperationType operationType, uint64_t fileSize, uint64_t bufferSize)
{
    operationType_ = operationType;
    fileDirect_ = -1;
    preAllocateFileSize_ = fileSize;
    globalWriteBuffer_ = new char[bufferSize];
    globalBufferSize_ = bufferSize;
    bufferUsedSize_ = 0;
}

FileOperation::~FileOperation()
{
    delete[] globalWriteBuffer_;
}

bool FileOperation::createFile(string path)
{
    if (operationType_ == kFstream) {
        fileStream_.open(path, ios::out);
        if (fileStream_.is_open() == false) {
            debug_error("[ERROR] File stream (create) error, path = %s\n", path.c_str());
            return false;
        } else {
            return true;
        }
        return true;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        fileDirect_ = open(path.c_str(), O_CREAT, 0644);
        if (fileDirect_ == -1) {
            debug_error("[ERROR] File descriptor (create) = %d, err = %s\n", fileDirect_, strerror(errno));
            return false;
        } else {
            newlyCreatedFileFlag_ = true;
            return true;
        }
    } else {
        return false;
    }
}

bool FileOperation::openFile(string path)
{
    if (operationType_ == kFstream) {
        fileStream_.open(path, ios::in | ios::out | ios::binary);
        if (fileStream_.is_open() == false) {
            debug_error("[ERROR] File stream (create) error, path = %s\n", path.c_str());
            return false;
        } else {
            return true;
        }
    } else if (operationType_ == kDirectIO) {
        fileDirect_ = open(path.c_str(), O_RDWR | O_DIRECT, 0644);
        if (fileDirect_ == -1) {
            debug_error("[ERROR] File descriptor (open) = %d, err = %s\n", fileDirect_, strerror(errno));
            return false;
        } else {
            if (newlyCreatedFileFlag_ == true) {
                directIOWriteFileSize_ = 0;
                directIOActualWriteFileSize_ = 0;
                debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fileDirect_, directIOWriteFileSize_, directIOActualWriteFileSize_);
            } else {
                directIOWriteFileSize_ = getFilePhysicalSize(path);
                directIOActualWriteFileSize_ = getFileSize();
                debug_info("Open old file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fileDirect_, directIOWriteFileSize_, directIOActualWriteFileSize_);
            }
            return true;
        }
    } else if (operationType_ == kAlignLinuxIO) {
        fileDirect_ = open(path.c_str(), O_RDWR, 0644);
        if (fileDirect_ == -1) {
            debug_error("[ERROR] File descriptor (open) = %d, err = %s\n", fileDirect_, strerror(errno));
            return false;
        } else {
            if (newlyCreatedFileFlag_ == true) {
                directIOWriteFileSize_ = 0;
                directIOActualWriteFileSize_ = 0;
                debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fileDirect_, directIOWriteFileSize_, directIOActualWriteFileSize_);
            } else {
                directIOWriteFileSize_ = getFilePhysicalSize(path);
                directIOActualWriteFileSize_ = getFileSize();
                debug_info("Open old file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fileDirect_, directIOWriteFileSize_, directIOActualWriteFileSize_);
            }
            return true;
        }
    } else {
        return false;
    }
}

bool FileOperation::createThenOpenFile(string path)
{
    switch (operationType_) {
    case kFstream:
        fileStream_.open(path, ios::out);
        if (fileStream_.is_open() == false) {
            debug_error("[ERROR] File stream (create) error, path = %s\n", path.c_str());
            return false;
        } else {
            fileStream_.close();
            fileStream_.open(path, ios::in | ios::out | ios::binary);
            if (fileStream_.is_open() == false) {
                debug_error("[ERROR] File stream (create) error, path = %s\n", path.c_str());
                return false;
            } else {
                return true;
            }
        }
        return true;
        break;
    case kDirectIO:
        fileDirect_ = open(path.c_str(), O_CREAT | O_RDWR | O_DIRECT, 0644);
        if (fileDirect_ == -1) {
            debug_error("[ERROR] File descriptor (open) = %d, err = %s\n", fileDirect_, strerror(errno));
            exit(1);
            return false;
        } else {
            int allocateStatus = fallocate(fileDirect_, 0, 0, preAllocateFileSize_);
            if (allocateStatus != 0) {
                debug_warn("[WARN] Could not pre-allocate space for current file: %s", path.c_str());
            }
            directIOWriteFileSize_ = 0;
            directIOActualWriteFileSize_ = 0;
            debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fileDirect_, directIOWriteFileSize_, directIOActualWriteFileSize_);
            return true;
        }
        break;
    case kAlignLinuxIO:
        fileDirect_ = open(path.c_str(), O_CREAT | O_RDWR, 0644);
        if (fileDirect_ == -1) {
            debug_error("[ERROR] File descriptor (open) = %d, err = %s\n", fileDirect_, strerror(errno));
            exit(1);
            return false;
        } else {
            directIOWriteFileSize_ = 0;
            directIOActualWriteFileSize_ = 0;
            debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fileDirect_, directIOWriteFileSize_, directIOActualWriteFileSize_);
            return true;
        }
        break;
    default:
        return false;
        break;
    }
}

bool FileOperation::closeFile()
{
    if (operationType_ == kFstream) {
        fileStream_.flush();
        fileStream_.close();
        return true;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        memset(globalWriteBuffer_, 0, globalBufferSize_);
        bufferUsedSize_ = 0;
        debug_info("Close file fd = %d\n", fileDirect_);
        int status = close(fileDirect_);
        if (status == 0) {
            debug_info("Close file success, current file fd = %d\n", fileDirect_);
            fileDirect_ = -1;
            return true;
        } else {
            debug_error("[ERROR] File descriptor (close) = %d, err = %s\n", fileDirect_, strerror(errno));
            return false;
        }
    } else {
        return false;
    }
}

bool FileOperation::isFileOpen()
{
    if (operationType_ == kFstream) {
        return fileStream_.is_open();
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (fileDirect_ == -1) {
            return false;
        } else {
            return true;
        }
    } else {
        return false;
    }
}

uint64_t FileOperation::getFileBufferedSize()
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    return bufferUsedSize_;
}

fileOperationStatus_t FileOperation::writeFile(char* contentBuffer, uint64_t contentSize)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::end);
        fileStream_.seekp(0, ios::end);
        fileStream_.write(contentBuffer, contentSize);
        fileOperationStatus_t ret(true, contentSize, contentSize, 0);
        return ret;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (contentSize + bufferUsedSize_ <= globalBufferSize_) {
            memcpy(globalWriteBuffer_ + bufferUsedSize_, contentBuffer, contentSize);
            bufferUsedSize_ += contentSize;
            fileOperationStatus_t ret(true, 0, 0, contentSize);
            return ret;
        } else {
            uint64_t targetRequestPageNumber = ceil((double)(contentSize + bufferUsedSize_) / (double)(directIOPageSize_ - sizeof(uint32_t)));
            uint64_t writeDoneContentSize = 0;
            // align mem
            char* writeBuffer;
            auto writeBufferSize = directIOPageSize_ * targetRequestPageNumber;
            auto ret = posix_memalign((void**)&writeBuffer, directIOPageSize_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                fileOperationStatus_t ret(false, 0, 0, 0);
                return ret;
            } else {
                memset(writeBuffer, 0, writeBufferSize);
            }
            uint64_t processedPageNumber = 0;
            uint64_t targetWriteSize = bufferUsedSize_ + contentSize;

            uint64_t previousBufferUsedSize = bufferUsedSize_;
            char contentBufferWithExistWriteBuffer[targetWriteSize];
            memcpy(contentBufferWithExistWriteBuffer, globalWriteBuffer_, previousBufferUsedSize);
            memcpy(contentBufferWithExistWriteBuffer + previousBufferUsedSize, contentBuffer, contentSize);
            int actualNeedWriteSize = 0;
            uint32_t currentPageWriteSize = 0;
            while (writeDoneContentSize != targetWriteSize) {
                if ((targetWriteSize - writeDoneContentSize) >= (directIOPageSize_ - sizeof(uint32_t))) {
                    currentPageWriteSize = directIOPageSize_ - sizeof(uint32_t);
                    memcpy(writeBuffer + processedPageNumber * directIOPageSize_, &currentPageWriteSize, sizeof(uint32_t));
                    memcpy(writeBuffer + processedPageNumber * directIOPageSize_ + sizeof(uint32_t), contentBufferWithExistWriteBuffer + writeDoneContentSize, currentPageWriteSize);
                    writeDoneContentSize += currentPageWriteSize;
                    actualNeedWriteSize += directIOPageSize_;
                    processedPageNumber++;
                } else {
                    currentPageWriteSize = targetWriteSize - writeDoneContentSize;
                    memset(globalWriteBuffer_, 0, globalBufferSize_);
                    memcpy(globalWriteBuffer_, contentBufferWithExistWriteBuffer + writeDoneContentSize, currentPageWriteSize);
                    bufferUsedSize_ = currentPageWriteSize;
                    writeDoneContentSize += currentPageWriteSize;
                    processedPageNumber++;
                }
            }
            if (currentPageWriteSize == (directIOPageSize_ - sizeof(uint32_t))) {
                memset(globalWriteBuffer_, 0, globalBufferSize_);
                bufferUsedSize_ = 0;
            }
            auto wReturn = pwrite(fileDirect_, writeBuffer, actualNeedWriteSize, directIOWriteFileSize_);
            if (wReturn != actualNeedWriteSize) {
                free(writeBuffer);
                debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fileDirect_, strerror(errno));
                fileOperationStatus_t ret(false, 0, 0, 0);
                return ret;
            } else {
                free(writeBuffer);
                directIOWriteFileSize_ += actualNeedWriteSize;
                directIOActualWriteFileSize_ += (targetWriteSize - bufferUsedSize_);
                if (directIOActualWriteFileSize_ == 542115) {
                    debug_error("targetWriteSize %lu bufferUsedSize_ %lu\n", targetWriteSize, bufferUsedSize_);
                }
                // cerr << "Content write size = " << (targetWriteSize - bufferUsedSize_) << ", request write size = " << contentSize << ", buffered size = " << bufferUsedSize_ << endl;
                fileOperationStatus_t ret(true, actualNeedWriteSize, targetWriteSize - bufferUsedSize_, bufferUsedSize_);
                return ret;
            }
        }
    } else {
        fileOperationStatus_t ret(false, 0, 0, 0);
        return ret;
    }
}

fileOperationStatus_t FileOperation::readFile(char* contentBuffer, uint64_t contentSize)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::beg);
        fileStream_.read(contentBuffer, contentSize);
        fileOperationStatus_t ret(true, contentSize, contentSize, 0);
        return ret;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (contentSize != directIOActualWriteFileSize_ + bufferUsedSize_) {
            debug_error("[ERROR] Read size mismatch, request size = %lu, DirectIO current writed physical size = %lu, actual size = %lu, buffered size = %lu\n", contentSize, directIOWriteFileSize_, directIOActualWriteFileSize_, bufferUsedSize_);
            fileOperationStatus_t ret(false, 0, 0, 0);
            return ret;
        }
        uint64_t targetRequestPageNumber = ceil((double)directIOWriteFileSize_ / (double)directIOPageSize_);
        // align mem
        char* readBuffer;
        auto readBufferSize = directIOPageSize_ * targetRequestPageNumber;
        auto ret = posix_memalign((void**)&readBuffer, directIOPageSize_, readBufferSize);
        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            fileOperationStatus_t ret(false, 0, 0, 0);
            return ret;
        }
        auto rReturn = pread(fileDirect_, readBuffer, readBufferSize, 0);
        if (rReturn != readBufferSize) {
            free(readBuffer);
            debug_error("[ERROR] Read return value = %lu, file fd = %d, err = %s, targetRequestPageNumber = %lu, readBuffer size = %lu, directIOWriteFileSize_ = %lu\n", rReturn, fileDirect_, strerror(errno), targetRequestPageNumber, readBufferSize, directIOWriteFileSize_);
            fileOperationStatus_t ret(false, 0, 0, 0);
            return ret;
        }
        uint64_t currentReadDoneSize = 0;
        vector<uint64_t> vec;
        for (auto processedPageNumber = 0; processedPageNumber < targetRequestPageNumber; processedPageNumber++) {
            uint32_t currentPageContentSize = 0;
            memcpy(&currentPageContentSize, readBuffer + processedPageNumber * directIOPageSize_, sizeof(uint32_t));
            memcpy(contentBuffer + currentReadDoneSize, readBuffer + processedPageNumber * directIOPageSize_ + sizeof(uint32_t), currentPageContentSize);
            currentReadDoneSize += currentPageContentSize;
            vec.push_back(currentReadDoneSize);
        }
        if (bufferUsedSize_ != 0) {
            memcpy(contentBuffer + currentReadDoneSize, globalWriteBuffer_, bufferUsedSize_);
            currentReadDoneSize += bufferUsedSize_;
        }
        if (currentReadDoneSize != contentSize) {
            free(readBuffer);
            debug_error("[ERROR] Read size mismatch, read size = %lu, request size = %lu, DirectIO current page number = %lu, DirectIO current read physical size = %lu, actual size = %lu, buffered size = %lu\n", currentReadDoneSize, contentSize, targetRequestPageNumber, directIOWriteFileSize_, directIOActualWriteFileSize_, bufferUsedSize_);
            uint64_t last = 0;
            for (auto& it : vec) {
                fprintf(stderr, "%lu %lu %lu\n", it, it - last, 4096 - (it - last));
                last = it;
            }
            fileOperationStatus_t ret(false, 0, 0, 0);
            return ret;
        } else {
            free(readBuffer);
            fileOperationStatus_t ret(true, directIOWriteFileSize_, contentSize, 0);
            return ret;
        }
    } else {
        fileOperationStatus_t ret(false, 0, 0, 0);
        return ret;
    }
}

fileOperationStatus_t FileOperation::flushFile()
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kFstream) {
        fileStream_.flush();
        fileOperationStatus_t ret(true, 0, 0, 0);
        return ret;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (bufferUsedSize_ != 0) {
            uint64_t targetRequestPageNumber = ceil((double)bufferUsedSize_ / (double)(directIOPageSize_ - sizeof(uint32_t)));
            // align mem
            char* writeBuffer;
            auto writeBufferSize = directIOPageSize_ * targetRequestPageNumber;
            auto ret = posix_memalign((void**)&writeBuffer, directIOPageSize_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                fileOperationStatus_t ret(false, 0, 0, 0);
                return ret;
            } else {
                memset(writeBuffer, 0, writeBufferSize);
            }
            uint64_t processedPageNumber = 0;
            uint64_t targetWriteSize = bufferUsedSize_;
            uint64_t writeDoneContentSize = 0;
            while (writeDoneContentSize != targetWriteSize) {
                uint32_t currentPageWriteSize;
                if ((targetWriteSize - writeDoneContentSize) >= (directIOPageSize_ - sizeof(uint32_t))) {
                    currentPageWriteSize = directIOPageSize_ - sizeof(uint32_t);
                    memcpy(writeBuffer + processedPageNumber * directIOPageSize_, &currentPageWriteSize, sizeof(uint32_t));
                    memcpy(writeBuffer + processedPageNumber * directIOPageSize_ + sizeof(uint32_t), globalWriteBuffer_ + writeDoneContentSize, currentPageWriteSize);
                    writeDoneContentSize += currentPageWriteSize;
                    processedPageNumber++;
                } else {
                    currentPageWriteSize = targetWriteSize - writeDoneContentSize;
                    memcpy(writeBuffer + processedPageNumber * directIOPageSize_, &currentPageWriteSize, sizeof(uint32_t));
                    memcpy(writeBuffer + processedPageNumber * directIOPageSize_ + sizeof(uint32_t), globalWriteBuffer_ + writeDoneContentSize, currentPageWriteSize);
                    writeDoneContentSize += currentPageWriteSize;
                    processedPageNumber++;
                }
            }
            auto wReturn = pwrite(fileDirect_, writeBuffer, writeBufferSize, directIOWriteFileSize_);
            if (wReturn != writeBufferSize) {
                free(writeBuffer);
                debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fileDirect_, strerror(errno));
                fileOperationStatus_t ret(false, 0, 0, 0);
                return ret;
            } else {
                free(writeBuffer);
                directIOWriteFileSize_ += writeBufferSize;
                directIOActualWriteFileSize_ += bufferUsedSize_;
                uint64_t flushedSize = bufferUsedSize_;
                bufferUsedSize_ = 0;
                memset(globalWriteBuffer_, 0, globalBufferSize_);
                fileOperationStatus_t ret(true, writeBufferSize, flushedSize, 0);
                return ret;
            }
        }
        fileOperationStatus_t ret(true, 0, 0, 0);
        return ret;
    } else {
        fileOperationStatus_t ret(false, 0, 0, 0);
        return ret;
    }
}

uint64_t FileOperation::getFileSize()
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::end);
        uint64_t fileSize = fileStream_.tellg();
        fileStream_.seekg(0, ios::beg);
        return fileSize;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        uint64_t fileRealSizeWithoutPadding = 0;
        uint64_t targetRequestPageNumber = ceil((double)directIOWriteFileSize_ / (double)directIOPageSize_);
        // align mem
        char* readBuffer;
        auto readBufferSize = directIOPageSize_ * targetRequestPageNumber;
        auto ret = posix_memalign((void**)&readBuffer, directIOPageSize_, readBufferSize);
        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            return false;
        }
        auto rReturn = pread(fileDirect_, readBuffer, readBufferSize, 0);
        if (rReturn != readBufferSize) {
            free(readBuffer);
            debug_error("[ERROR] Read return value = %lu, err = %s, targetRequestPageNumber = %lu, readBuffer size = %lu, directIOWriteFileSize_ = %lu\n", rReturn, strerror(errno), targetRequestPageNumber, readBufferSize, directIOWriteFileSize_);
            free(readBuffer);
            return false;
        }
        for (auto processedPageNumber = 0; processedPageNumber < targetRequestPageNumber; processedPageNumber++) {
            uint32_t currentPageContentSize = 0;
            memcpy(&currentPageContentSize, readBuffer + processedPageNumber * directIOPageSize_, sizeof(uint32_t));
            fileRealSizeWithoutPadding += currentPageContentSize;
        }
        free(readBuffer);
        return fileRealSizeWithoutPadding;
    } else {
        return 0;
    }
}

uint64_t FileOperation::getFilePhysicalSize(string path)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    struct stat statbuf;
    stat(path.c_str(), &statbuf);
    uint64_t physicalFileSize = statbuf.st_size;
    return physicalFileSize;
}

} // namespace DELTAKV_NAMESPACE
