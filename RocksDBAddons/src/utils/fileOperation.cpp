#include "utils/fileOperation.hpp"

using namespace std;

namespace DELTAKV_NAMESPACE {

FileOperation::FileOperation(fileOperationType operationType)
{
    operationType_ = operationType;
    fileDirect_ = -1;
    preAllocateFileSize_ = 256 * 1024;
    globalWriteBuffer_ = new char[directIOPageSize_];
    globalBufferSize_ = directIOPageSize_;
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
    delete globalWriteBuffer_;
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

uint64_t FileOperation::writeFile(char* contentBuffer, uint64_t contentSize)
{
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::end);
        fileStream_.seekp(0, ios::end);
        fileStream_.write(contentBuffer, contentSize);
        return contentSize;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (contentSize + bufferUsedSize_ <= globalBufferSize_) {
            memcpy(globalWriteBuffer_ + bufferUsedSize_, contentBuffer, contentSize);
            bufferUsedSize_ += contentSize;
            return contentSize;
        } else {
            uint64_t targetRequestPageNumber = ceil((double)contentSize + bufferUsedSize_ / (double)(directIOPageSize_ - sizeof(uint32_t)));
            uint64_t writeDoneContentSize = 0;
            // align mem
            char* writeBuffer;
            auto writeBufferSize = directIOPageSize_ * targetRequestPageNumber;
            auto ret = posix_memalign((void**)&writeBuffer, directIOPageSize_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                return 0;
            } else {
                memset(writeBuffer, 0, writeBufferSize);
            }
            uint64_t processedPageNumber = 0;
            uint64_t targetWriteSize = bufferUsedSize_ + contentSize;
            if (targetWriteSize != contentSize) {
                char contentBufferWithExistWriteBuffer[targetWriteSize];
                memcpy(contentBufferWithExistWriteBuffer, globalWriteBuffer_, bufferUsedSize_);
                memcpy(contentBufferWithExistWriteBuffer + bufferUsedSize_, contentBuffer, contentSize);
                int actualNeedWriteSize = 0;
                while (writeDoneContentSize != targetWriteSize) {
                    uint32_t currentPageWriteSize;
                    if ((targetWriteSize - writeDoneContentSize) > (directIOPageSize_ - sizeof(uint32_t))) {
                        currentPageWriteSize = directIOPageSize_ - sizeof(uint32_t);
                        memcpy(writeBuffer + processedPageNumber * directIOPageSize_, &currentPageWriteSize, sizeof(uint32_t));
                        memcpy(writeBuffer + processedPageNumber * directIOPageSize_ + sizeof(uint32_t), contentBufferWithExistWriteBuffer + writeDoneContentSize, currentPageWriteSize);
                        writeDoneContentSize += currentPageWriteSize;
                        actualNeedWriteSize += directIOPageSize_;
                        processedPageNumber++;
                    } else {
                        currentPageWriteSize = targetWriteSize - writeDoneContentSize;
                        memcpy(globalWriteBuffer_, contentBufferWithExistWriteBuffer + writeDoneContentSize, currentPageWriteSize);
                        bufferUsedSize_ = currentPageWriteSize;
                        writeDoneContentSize += currentPageWriteSize;
                        processedPageNumber++;
                    }
                }
                auto wReturn = pwrite(fileDirect_, writeBuffer, actualNeedWriteSize, directIOWriteFileSize_);
                if (wReturn != actualNeedWriteSize) {
                    free(writeBuffer);
                    debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fileDirect_, strerror(errno));
                    return 0;
                } else {
                    free(writeBuffer);
                    directIOWriteFileSize_ += actualNeedWriteSize;
                    directIOActualWriteFileSize_ += contentSize;
                    return actualNeedWriteSize;
                }
            } else {
                int actualNeedWriteSize = 0;
                while (writeDoneContentSize != targetWriteSize) {
                    uint32_t currentPageWriteSize;
                    if ((targetWriteSize - writeDoneContentSize) > (directIOPageSize_ - sizeof(uint32_t))) {
                        currentPageWriteSize = directIOPageSize_ - sizeof(uint32_t);
                        memcpy(writeBuffer + processedPageNumber * directIOPageSize_, &currentPageWriteSize, sizeof(uint32_t));
                        memcpy(writeBuffer + processedPageNumber * directIOPageSize_ + sizeof(uint32_t), contentBuffer + writeDoneContentSize, currentPageWriteSize);
                        writeDoneContentSize += currentPageWriteSize;
                        actualNeedWriteSize += directIOPageSize_;
                        processedPageNumber++;
                    } else {
                        currentPageWriteSize = targetWriteSize - writeDoneContentSize;
                        memcpy(globalWriteBuffer_, contentBuffer + writeDoneContentSize, currentPageWriteSize);
                        bufferUsedSize_ = currentPageWriteSize;
                        writeDoneContentSize += currentPageWriteSize;
                        processedPageNumber++;
                    }
                }
                auto wReturn = pwrite(fileDirect_, writeBuffer, actualNeedWriteSize, directIOWriteFileSize_);
                if (wReturn != actualNeedWriteSize) {
                    free(writeBuffer);
                    debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fileDirect_, strerror(errno));
                    return 0;
                } else {
                    free(writeBuffer);
                    directIOWriteFileSize_ += actualNeedWriteSize;
                    directIOActualWriteFileSize_ += contentSize;
                    return actualNeedWriteSize;
                }
            }
        }
    } else {
        return 0;
    }
}

bool FileOperation::readFile(char* contentBuffer, uint64_t contentSize)
{
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::beg);
        fileStream_.read(contentBuffer, contentSize);
        return true;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (bufferUsedSize_ != 0) {
            debug_error("[ERROR] Read when buffer not flushed, size = %d\n", bufferUsedSize_);
            return false;
        }
        uint64_t targetRequestPageNumber = ceil((double)directIOWriteFileSize_ / (double)directIOPageSize_);
        uint64_t readDoneContentSize = 0;
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
            debug_error("[ERROR] Read return value = %lu, file fd = %d, err = %s, targetRequestPageNumber = %lu, readBuffer size = %lu, directIOWriteFileSize_ = %lu\n", rReturn, fileDirect_, strerror(errno), targetRequestPageNumber, readBufferSize, directIOWriteFileSize_);
            return false;
        }
        uint64_t currentReadDoneSize = 0;
        for (auto processedPageNumber = 0; processedPageNumber < targetRequestPageNumber; processedPageNumber++) {
            uint32_t currentPageContentSize = 0;
            memcpy(&currentPageContentSize, readBuffer + processedPageNumber * directIOPageSize_, sizeof(uint32_t));
            memcpy(contentBuffer + currentReadDoneSize, readBuffer + processedPageNumber * directIOPageSize_ + sizeof(uint32_t), currentPageContentSize);
            currentReadDoneSize += currentPageContentSize;
        }
        if (currentReadDoneSize != contentSize) {
            free(readBuffer);
            debug_error("[ERROR] Read size mismatch, read size = %lu, request size = %lu, DirectIO current page number = %lu, DirectIO current write physical size = %lu, actual size = %lu\n", currentReadDoneSize, contentSize, targetRequestPageNumber, directIOWriteFileSize_, directIOActualWriteFileSize_);
            return false;
        } else {
            free(readBuffer);
            return true;
        }
    } else {
        return false;
    }
}

pair<uint64_t, uint64_t> FileOperation::flushFile()
{
    if (operationType_ == kFstream) {
        fileStream_.flush();
        return make_pair(0, 0);
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (bufferUsedSize_ != 0) {
            uint64_t targetRequestPageNumber = ceil((double)bufferUsedSize_ / (double)(directIOPageSize_ - sizeof(uint32_t)));
            uint64_t writeDoneContentSize = 0;
            // align mem
            char* writeBuffer;
            auto writeBufferSize = directIOPageSize_ * targetRequestPageNumber;
            auto ret = posix_memalign((void**)&writeBuffer, directIOPageSize_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                return make_pair(0, 0);
            } else {
                memset(writeBuffer, 0, writeBufferSize);
            }
            uint64_t processedPageNumber = 0;
            uint64_t targetWriteSize = bufferUsedSize_;

            while (writeDoneContentSize != targetWriteSize) {
                uint32_t currentPageWriteSize;
                if ((targetWriteSize - writeDoneContentSize) > (directIOPageSize_ - sizeof(uint32_t))) {
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
                return make_pair(0, 0);
            } else {
                free(writeBuffer);
                directIOWriteFileSize_ += writeBufferSize;
                directIOActualWriteFileSize_ += bufferUsedSize_;
                uint64_t flushedSize = bufferUsedSize_;
                bufferUsedSize_ = 0;
                memset(globalWriteBuffer_, 0, directIOPageSize_);
                return make_pair(writeBufferSize, flushedSize);
            }
        }
        return make_pair(0, 0);
    } else {
        return make_pair(0, 0);
    }
}

uint64_t FileOperation::getFileSize()
{
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
    struct stat statbuf;
    stat(path.c_str(), &statbuf);
    uint64_t physicalFileSize = statbuf.st_size;
    return physicalFileSize;
}

} // namespace DELTAKV_NAMESPACE