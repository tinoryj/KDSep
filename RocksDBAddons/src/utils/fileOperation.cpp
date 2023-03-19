#include "utils/fileOperation.hpp"
#include "utils/statsRecorder.hh"

using namespace std;

namespace DELTAKV_NAMESPACE {

FileOperation::FileOperation(fileOperationType operationType)
{
    operationType_ = operationType;
    fd_ = -1;
    preAllocateFileSize_ = 256 * 1024;
    globalWriteBuffer_ = new char[page_size_ - sizeof(uint32_t)];
    globalBufferSize_ = page_size_ - sizeof(uint32_t);
    buf_used_size_ = 0;
}

FileOperation::FileOperation(fileOperationType operationType, uint64_t fileSize, uint64_t bufferSize)
{
    operationType_ = operationType;
    fd_ = -1;
    preAllocateFileSize_ = fileSize;
    globalWriteBuffer_ = new char[bufferSize];
    globalBufferSize_ = bufferSize;
    buf_used_size_ = 0;
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
        fd_ = open(path.c_str(), O_CREAT, 0644);
        if (fd_ == -1) {
            debug_error("[ERROR] File descriptor (create) = %d, err = %s\n", fd_, strerror(errno));
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
        fd_ = open(path.c_str(), O_RDWR | O_DIRECT, 0644);
        if (fd_ == -1) {
            debug_error("[ERROR] File descriptor (open) = %d, err = %s\n", fd_, strerror(errno));
            return false;
        } else {
            if (newlyCreatedFileFlag_ == true) {
                direct_io_disk_size_ = 0;
                direct_io_data_size_ = 0;
                debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, direct_io_disk_size_, direct_io_data_size_);
            } else {
                direct_io_disk_size_ = getFilePhysicalSize(path);
                direct_io_data_size_ = getFileSize();
                debug_info("Open old file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, direct_io_disk_size_, direct_io_data_size_);
            }
            return true;
        }
    } else if (operationType_ == kAlignLinuxIO) {
        fd_ = open(path.c_str(), O_RDWR, 0644);
        if (fd_ == -1) {
            debug_error("[ERROR] File descriptor (open) = %d, err = %s\n", fd_, strerror(errno));
            return false;
        } else {
            if (newlyCreatedFileFlag_ == true) {
                direct_io_disk_size_ = 0;
                direct_io_data_size_ = 0;
                debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, direct_io_disk_size_, direct_io_data_size_);
            } else {
                direct_io_disk_size_ = getFilePhysicalSize(path);
                direct_io_data_size_ = getFileSize();
                debug_info("Open old file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, direct_io_disk_size_, direct_io_data_size_);
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
        fd_ = open(path.c_str(), O_CREAT | O_RDWR | O_DIRECT, 0644);
        if (fd_ == -1) {
            debug_error("[ERROR] File descriptor (open) = %d, err = %s\n", fd_, strerror(errno));
            exit(1);
            return false;
        } else {
            int allocateStatus = fallocate(fd_, 0, 0, preAllocateFileSize_);
            if (allocateStatus != 0) {
                debug_warn("[WARN] Could not pre-allocate space for current file: %s", path.c_str());
            }
            direct_io_disk_size_ = 0;
            direct_io_data_size_ = 0;
            debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, direct_io_disk_size_, direct_io_data_size_);
            return true;
        }
        break;
    case kAlignLinuxIO:
        fd_ = open(path.c_str(), O_CREAT | O_RDWR, 0644);
        if (fd_ == -1) {
            debug_error("[ERROR] File descriptor (open) = %d, err = %s\n", fd_, strerror(errno));
            exit(1);
            return false;
        } else {
            direct_io_disk_size_ = 0;
            direct_io_data_size_ = 0;
            debug_info("Open new file at path = %s, file fd = %d, current physical file size = %lu, actual file size = %lu\n", path.c_str(), fd_, direct_io_disk_size_, direct_io_data_size_);
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
        buf_used_size_ = 0;
        debug_info("Close file fd = %d\n", fd_);
        int status = close(fd_);
        if (status == 0) {
            debug_info("Close file success, current file fd = %d\n", fd_);
            fd_ = -1;
            return true;
        } else {
            debug_error("[ERROR] File descriptor (close) = %d, err = %s\n", fd_, strerror(errno));
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
        if (fd_ == -1) {
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
    return buf_used_size_;
}

FileOpStatus FileOperation::writeFile(char* contentBuffer, uint64_t contentSize)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::end);
        fileStream_.seekp(0, ios::end);
        fileStream_.write(contentBuffer, contentSize);
        FileOpStatus ret(true, contentSize, contentSize, 0);
        return ret;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (contentSize + buf_used_size_ <= globalBufferSize_) {
            memcpy(globalWriteBuffer_ + buf_used_size_, contentBuffer, contentSize);
            buf_used_size_ += contentSize;
            FileOpStatus ret(true, 0, 0, contentSize);
            return ret;
        } else {
            uint64_t req_page_num = ceil((double)(contentSize + buf_used_size_) / (double)(page_size_ - sizeof(uint32_t)));
            uint64_t writeDoneContentSize = 0;
            // align mem
            char* writeBuffer;
            auto writeBufferSize = page_size_ * req_page_num;
            auto ret = posix_memalign((void**)&writeBuffer, page_size_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                memset(writeBuffer, 0, writeBufferSize);
            }
            uint64_t processedPageNumber = 0;
            uint64_t targetWriteSize = buf_used_size_ + contentSize;

            uint64_t previousBufferUsedSize = buf_used_size_;
            char contentBufferWithExistWriteBuffer[targetWriteSize];
            memcpy(contentBufferWithExistWriteBuffer, globalWriteBuffer_, previousBufferUsedSize);
            memcpy(contentBufferWithExistWriteBuffer + previousBufferUsedSize, contentBuffer, contentSize);
            int actualNeedWriteSize = 0;
            uint32_t currentPageWriteSize = 0;
            while (writeDoneContentSize != targetWriteSize) {
                if ((targetWriteSize - writeDoneContentSize) >= (page_size_ - sizeof(uint32_t))) {
                    currentPageWriteSize = page_size_ - sizeof(uint32_t);
                    memcpy(writeBuffer + processedPageNumber * page_size_, &currentPageWriteSize, sizeof(uint32_t));
                    memcpy(writeBuffer + processedPageNumber * page_size_ + sizeof(uint32_t), contentBufferWithExistWriteBuffer + writeDoneContentSize, currentPageWriteSize);
                    writeDoneContentSize += currentPageWriteSize;
                    actualNeedWriteSize += page_size_;
                    processedPageNumber++;
                } else {
                    currentPageWriteSize = targetWriteSize - writeDoneContentSize;
                    memset(globalWriteBuffer_, 0, globalBufferSize_);
                    memcpy(globalWriteBuffer_, contentBufferWithExistWriteBuffer + writeDoneContentSize, currentPageWriteSize);
                    buf_used_size_ = currentPageWriteSize;
                    writeDoneContentSize += currentPageWriteSize;
                    processedPageNumber++;
                }
            }
            if (currentPageWriteSize == (page_size_ - sizeof(uint32_t))) {
                memset(globalWriteBuffer_, 0, globalBufferSize_);
                buf_used_size_ = 0;
            }
            struct timeval tv;
            gettimeofday(&tv, 0);
            auto wReturn = pwrite(fd_, writeBuffer, actualNeedWriteSize, direct_io_disk_size_);
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_FILE_FUNC_REAL_WRITE, tv);
            if (wReturn != actualNeedWriteSize) {
                free(writeBuffer);
                debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                free(writeBuffer);
                direct_io_disk_size_ += actualNeedWriteSize;
                direct_io_data_size_ += (targetWriteSize - buf_used_size_);
                // cerr << "Content write size = " << (targetWriteSize - buf_used_size_) << ", request write size = " << contentSize << ", buffered size = " << buf_used_size_ << endl;
                FileOpStatus ret(true, actualNeedWriteSize, targetWriteSize - buf_used_size_, buf_used_size_);
                return ret;
            }
        }
    } else {
        FileOpStatus ret(false, 0, 0, 0);
        return ret;
    }
}

FileOpStatus FileOperation::readFile(char* contentBuffer, uint64_t contentSize)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kFstream) {
        fileStream_.seekg(0, ios::beg);
        fileStream_.read(contentBuffer, contentSize);
        FileOpStatus ret(true, contentSize, contentSize, 0);
        return ret;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (contentSize != direct_io_data_size_ + buf_used_size_) {
            debug_error("[ERROR] Read size mismatch, request size = %lu,"
                    " DirectIO current writed physical size = %lu, actual size"
                    " = %lu, buffered size = %lu\n", 
                    contentSize, direct_io_disk_size_, direct_io_data_size_,
                    buf_used_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        uint64_t req_page_num = ceil((double)direct_io_disk_size_ / (double)page_size_);
        // align mem
        char* readBuffer;
        auto readBufferSize = page_size_ * req_page_num;
        auto ret = posix_memalign((void**)&readBuffer, page_size_, readBufferSize);
        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        auto rReturn = pread(fd_, readBuffer, readBufferSize, 0);
        if (rReturn != readBufferSize) {
            free(readBuffer);
            debug_error("[ERROR] Read return value = %lu, file fd = %d, err = %s, req_page_num = %lu, readBuffer size = %lu, direct_io_disk_size_ = %lu\n", rReturn, fd_, strerror(errno), req_page_num, readBufferSize, direct_io_disk_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        uint64_t currentReadDoneSize = 0;
        vector<uint64_t> vec;
        for (auto processedPageNumber = 0; processedPageNumber < req_page_num; processedPageNumber++) {
            uint32_t page_data_size = 0;
            memcpy(&page_data_size, readBuffer + processedPageNumber * page_size_, sizeof(uint32_t));
            memcpy(contentBuffer + currentReadDoneSize, readBuffer + processedPageNumber * page_size_ + sizeof(uint32_t), page_data_size);
            currentReadDoneSize += page_data_size;
            vec.push_back(currentReadDoneSize);
        }
        if (buf_used_size_ != 0) {
            memcpy(contentBuffer + currentReadDoneSize, globalWriteBuffer_, buf_used_size_);
            currentReadDoneSize += buf_used_size_;
        }
        if (currentReadDoneSize != contentSize) {
            free(readBuffer);
            debug_error("[ERROR] Read size mismatch, read size = %lu, request size = %lu, DirectIO current page number = %lu, DirectIO current read physical size = %lu, actual size = %lu, buffered size = %lu\n", currentReadDoneSize, contentSize, req_page_num, direct_io_disk_size_, direct_io_data_size_, buf_used_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        } else {
            free(readBuffer);
            FileOpStatus ret(true, direct_io_disk_size_, contentSize, 0);
            return ret;
        }
    } else {
        FileOpStatus ret(false, 0, 0, 0);
        return ret;
    }
}

FileOpStatus FileOperation::positionedReadFile(char* read_buf, 
        uint64_t offset, uint64_t read_buf_size)
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kFstream) {
        debug_error("[ERROR] kFstream not implemented (sz %lu)\n", 
                read_buf_size);
        return FileOpStatus(false, 0, 0, 0);
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (read_buf_size == 0) {
            debug_error("read size cannot be zero!%s\n", "");
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        if (offset + read_buf_size > direct_io_data_size_ + buf_used_size_) {
            debug_error("[ERROR] Read size mismatch, request size = %lu,"
                    " DirectIO current writed physical size %lu, actual size"
                    " %lu, buffered size %lu\n", read_buf_size,
                    direct_io_disk_size_, direct_io_data_size_,
                    buf_used_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }

        // data offsets
        uint64_t offset_end = offset + read_buf_size;

        // if both are 0, don't need to read from the buffer
        uint64_t disk_start_page_id; 
        uint64_t disk_end_page_id;

        // buf_end_offset: The offset for read in the buffer
        // if it is 0, don't need to read from the buffer

        // req_disk_data_size: The data size on disk
        // req_buf_data_size: The data size in buffer 
        // req_disk_data_size + req_buf_data_size == read_buf_size
        uint64_t req_disk_data_size;
        uint64_t req_buf_data_size;

        if (offset >= direct_io_data_size_) {
            // The whole data in buffer
            memcpy(read_buf, 
                    globalWriteBuffer_ + offset - direct_io_data_size_,
                    read_buf_size); 
            return FileOpStatus(true, read_buf_size, read_buf_size, 0);
        } 

        if (offset + read_buf_size > direct_io_data_size_) {
            // Half data on disk, half data in buffer 
            req_buf_data_size = offset + read_buf_size - direct_io_data_size_;
            req_disk_data_size = read_buf_size - req_buf_data_size;
            disk_start_page_id = offset / data_page_size_; 
            disk_end_page_id = direct_io_data_size_ / data_page_size_;
        } else {
            // All data on disk. did nothing
            req_disk_data_size = read_buf_size;
            req_buf_data_size = 0;
            disk_start_page_id = offset / data_page_size_; 
            disk_end_page_id = (offset_end + data_page_size_ - 1) / data_page_size_;
        }

        uint64_t req_page_num = disk_end_page_id - disk_start_page_id;

        // align mem
        char* tmp_read_buf;
        auto readBufferSize = req_page_num * page_size_;

        if (req_page_num == 0) {
            debug_error("[ERROR] req page number should not be 0, "
                    "offset %lu size %lu direct io size %lu "
                    "req disk %lu req buf %lu\n", 
                    offset, read_buf_size, direct_io_data_size_,
                    req_disk_data_size, req_buf_data_size);
            exit(1);
        }

        auto ret = posix_memalign((void**)&tmp_read_buf, page_size_, readBufferSize);
        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }
        auto rReturn = pread(fd_, tmp_read_buf, readBufferSize, disk_start_page_id * page_size_);
        if (rReturn != readBufferSize) {
            free(tmp_read_buf);
            debug_error("[ERROR] Read return value = %lu, file fd = %d, err = %s, req_page_num = %lu, tmp_read_buf size = %lu, direct_io_disk_size_ = %lu\n", rReturn, fd_, strerror(errno), req_page_num, readBufferSize, direct_io_disk_size_);
            FileOpStatus ret(false, 0, 0, 0);
            return ret;
        }

        uint64_t left_size = req_disk_data_size; 
        // page_index: Where the page starts to read from tmp_read_buf to. It
        // is the index within the page.
        uint64_t page_index = offset - disk_start_page_id * data_page_size_;
        uint64_t read_done_size = 0;

        for (auto page_id = 0; page_id < req_page_num; page_id++) {
            uint32_t page_data_size = 0;

            // Read size in this page
            uint64_t read_size = data_page_size_ - page_index; 
            if (read_size > left_size) {
                read_size = left_size;
            }

            memcpy(&page_data_size, 
                    tmp_read_buf + page_id * page_size_, sizeof(uint32_t));
            memcpy(read_buf + read_done_size, 
                    tmp_read_buf + page_id * page_size_ + sizeof(uint32_t) + page_index, 
                    read_size);

            if (page_data_size != data_page_size_) {
                debug_error("[ERROR] on disk data size error: %u v.s."
                        " %lu\n", page_data_size, data_page_size_);
            } 

            read_done_size += read_size;
            left_size -= read_size;
            page_index = 0;
        }

        if (read_done_size != req_disk_data_size || left_size != 0) {
            debug_error("read done size mismatch: %lu v.s. %lu %lu\n",
                    read_done_size, req_disk_data_size, left_size);
            exit(1);
        }

        if (req_buf_data_size != 0) {
            memcpy(read_buf + read_done_size, 
                    globalWriteBuffer_, req_buf_data_size);
            read_done_size += req_buf_data_size;
        }

        free(tmp_read_buf);
        if (read_done_size != read_buf_size) {
            debug_error("[ERROR] Read size mismatch, read size = %lu, request size = %lu, DirectIO current page number = %lu, DirectIO current read physical size = %lu, actual size = %lu, buffered size = %lu\n", read_done_size, read_buf_size, req_page_num, direct_io_disk_size_, direct_io_data_size_, buf_used_size_);
            return FileOpStatus(false, 0, 0, 0);
        } else {
            return FileOpStatus(true, 
                    req_buf_data_size + req_page_num * page_size_,
                    read_buf_size, 0);
        }
    } else {
        FileOpStatus ret(false, 0, 0, 0);
        return ret;
    }
}

FileOpStatus FileOperation::flushFile()
{
    // std::scoped_lock<std::shared_mutex> w_lock(fileLock_);
    if (operationType_ == kFstream) {
        fileStream_.flush();
        FileOpStatus ret(true, 0, 0, 0);
        return ret;
    } else if (operationType_ == kDirectIO || operationType_ == kAlignLinuxIO) {
        if (buf_used_size_ != 0) {
            uint64_t req_page_num = ceil((double)buf_used_size_ / (double)(page_size_ - sizeof(uint32_t)));
            // align mem
            char* writeBuffer;
            auto writeBufferSize = page_size_ * req_page_num;
            auto ret = posix_memalign((void**)&writeBuffer, page_size_, writeBufferSize);
            if (ret) {
                debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                memset(writeBuffer, 0, writeBufferSize);
            }
            uint64_t processedPageNumber = 0;
            uint64_t targetWriteSize = buf_used_size_;
            uint64_t writeDoneContentSize = 0;
            while (writeDoneContentSize != targetWriteSize) {
                uint32_t currentPageWriteSize;
                if ((targetWriteSize - writeDoneContentSize) >= (page_size_ - sizeof(uint32_t))) {
                    currentPageWriteSize = page_size_ - sizeof(uint32_t);
                    memcpy(writeBuffer + processedPageNumber * page_size_, &currentPageWriteSize, sizeof(uint32_t));
                    memcpy(writeBuffer + processedPageNumber * page_size_ + sizeof(uint32_t), globalWriteBuffer_ + writeDoneContentSize, currentPageWriteSize);
                    writeDoneContentSize += currentPageWriteSize;
                    processedPageNumber++;
                } else {
                    currentPageWriteSize = targetWriteSize - writeDoneContentSize;
                    memcpy(writeBuffer + processedPageNumber * page_size_, &currentPageWriteSize, sizeof(uint32_t));
                    memcpy(writeBuffer + processedPageNumber * page_size_ + sizeof(uint32_t), globalWriteBuffer_ + writeDoneContentSize, currentPageWriteSize);
                    writeDoneContentSize += currentPageWriteSize;
                    processedPageNumber++;
                }
            }
            struct timeval tv;
            gettimeofday(&tv, 0);
            auto wReturn = pwrite(fd_, writeBuffer, writeBufferSize, direct_io_disk_size_);
            StatsRecorder::getInstance()->timeProcess(StatsType::DS_FILE_FUNC_REAL_FLUSH, tv);
            if (wReturn != writeBufferSize) {
                free(writeBuffer);
                debug_error("[ERROR] Write return value = %ld, file fd = %d, err = %s\n", wReturn, fd_, strerror(errno));
                FileOpStatus ret(false, 0, 0, 0);
                return ret;
            } else {
                free(writeBuffer);
                direct_io_disk_size_ += writeBufferSize;
                direct_io_data_size_ += buf_used_size_;
                uint64_t flushedSize = buf_used_size_;
                buf_used_size_ = 0;
                memset(globalWriteBuffer_, 0, globalBufferSize_);
                FileOpStatus ret(true, writeBufferSize, flushedSize, 0);
                return ret;
            }
        }
        FileOpStatus ret(true, 0, 0, 0);
        return ret;
    } else {
        FileOpStatus ret(false, 0, 0, 0);
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
        uint64_t req_page_num = ceil((double)direct_io_disk_size_ / (double)page_size_);
        // align mem
        char* readBuffer;
        auto readBufferSize = page_size_ * req_page_num;
        auto ret = posix_memalign((void**)&readBuffer, page_size_, readBufferSize);
        if (ret) {
            debug_error("[ERROR] posix_memalign failed: %d %s\n", errno, strerror(errno));
            return false;
        }
        auto rReturn = pread(fd_, readBuffer, readBufferSize, 0);
        if (rReturn != readBufferSize) {
            free(readBuffer);
            debug_error("[ERROR] Read return value = %lu, err = %s, req_page_num = %lu, readBuffer size = %lu, direct_io_disk_size_ = %lu\n", rReturn, strerror(errno), req_page_num, readBufferSize, direct_io_disk_size_);
            free(readBuffer);
            return false;
        }
        for (auto processedPageNumber = 0; processedPageNumber < req_page_num; processedPageNumber++) {
            uint32_t page_data_size = 0;
            memcpy(&page_data_size, readBuffer + processedPageNumber * page_size_, sizeof(uint32_t));
            fileRealSizeWithoutPadding += page_data_size;
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
