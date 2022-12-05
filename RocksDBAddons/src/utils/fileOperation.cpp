#include "utils/fileOperation.hpp"

using namespace std;

namespace DELTAKV_NAMESPACE {

FileOperation::FileOperation(fileOperationType operationType)
{
    operationType_ = operationType;
}

FileOperation::~FileOperation()
{
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
    } else if (operationType_ == kDirectIO) {
        fileDirect_ = open(path.c_str(), O_CREAT, 0644);
        if (fileDirect_ == -1) {
            debug_error("[ERROR] File descriptor (create) = %d, err = %s\n", fileDirect_, strerror(errno));
            return false;
        } else {
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
            directIOWriteFileSize_ = getFilePhysicalSize(path);
            return true;
        }
    } else {
        return false;
    }
}

bool FileOperation::closeFile()
{
    if (operationType_ == kFstream) {
        fileStream_.flush();
        fileStream_.close();
        return true;
    } else if (operationType_ == kDirectIO) {
        close(fileDirect_);
        return true;
    } else {
        return false;
    }
}

bool FileOperation::writeFile(char* contentBuffer, uint64_t contentSize)
{
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::end);
        fileStream_.seekp(0, ios::end);
        fileStream_.write(contentBuffer, contentSize);
        return true;
    } else if (operationType_ == kDirectIO) {
        uint64_t targetRequestPageNumber = ceil((double)contentSize / (double)(directIOPageSize_ - sizeof(uint32_t)));
        uint64_t writeDoneContentSize = 0;
        // align mem
        char* writeBuffer;
        auto writeBufferSize = directIOPageSize_ * targetRequestPageNumber;
        auto ret = posix_memalign((void**)&writeBuffer, directIOPageSize_, writeBufferSize);
        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            return false;
        } else {
            memset(writeBuffer, 0, writeBufferSize);
        }
        uint64_t processedPageNumber = 0;
        while (writeDoneContentSize != contentSize) {
            uint32_t currentPageWriteSize;
            if ((contentSize - writeDoneContentSize) > (directIOPageSize_ - sizeof(uint32_t))) {
                currentPageWriteSize = directIOPageSize_ - sizeof(uint32_t);
            } else {
                currentPageWriteSize = contentSize - writeDoneContentSize;
            }
            memcpy(writeBuffer + processedPageNumber * directIOPageSize_, &currentPageWriteSize, sizeof(uint32_t));
            memcpy(writeBuffer + processedPageNumber * directIOPageSize_ + sizeof(uint32_t), contentBuffer + writeDoneContentSize, currentPageWriteSize);
            writeDoneContentSize += currentPageWriteSize;
            processedPageNumber++;
        }
        auto wReturn = pwrite(fileDirect_, writeBuffer, writeBufferSize, directIOWriteFileSize_);
        if (wReturn != writeBufferSize) {
            free(writeBuffer);
            directIOWriteFileSize_ += wReturn;
            debug_error("[ERROR] Write return value = %ld, err = %s\n", wReturn, strerror(errno));
            return false;
        } else {
            free(writeBuffer);
            directIOWriteFileSize_ += writeBufferSize;
            return true;
        }
        return true;
    } else {
        return false;
    }
}

bool FileOperation::readFile(char* contentBuffer, uint64_t contentSize)
{
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::beg);
        fileStream_.read(contentBuffer, contentSize);
        return true;
    } else if (operationType_ == kDirectIO) {
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
            debug_error("[ERROR] Read return value = %lu, err = %s, targetRequestPageNumber = %lu, readBuffer size = %lu, directIOWriteFileSize_ = %lu\n", rReturn, strerror(errno), targetRequestPageNumber, readBufferSize, directIOWriteFileSize_);
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
            debug_error("[ERROR] Read size mismatch, read size = %lu, request size = %lu\n", currentReadDoneSize, contentSize);
            return false;
        } else {
            free(readBuffer);
            return true;
        }
    } else {
        return false;
    }
}

bool FileOperation::flushFile()
{
    if (operationType_ == kFstream) {
        fileStream_.flush();
        return true;
    } else if (operationType_ == kDirectIO) {
        return true;
    } else {
        return false;
    }
}

bool FileOperation::resetPointer(fileOperationSetPointerOps ops)
{
    if (operationType_ == kFstream) {
        if (ops == kBegin) {
            fileStream_.seekg(0, ios::beg);
            fileStream_.seekp(0, ios::beg);
        } else {
            fileStream_.seekg(0, ios::end);
            fileStream_.seekp(0, ios::end);
        }
        return true;
    } else if (operationType_ == kDirectIO) {
        return false;
    } else {
        return false;
    }
}

uint64_t FileOperation::getFileSize()
{
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::end);
        uint64_t fileSize = fileStream_.tellg();
        fileStream_.seekg(0, ios::beg);
        return fileSize;
    } else if (operationType_ == kDirectIO) {
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