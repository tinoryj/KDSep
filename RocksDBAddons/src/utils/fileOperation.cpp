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

bool FileOperation::create(string path)
{
    if (operationType_ == kFstream) {
        fileStream_.open(path, ios::out);
        if (fileStream_.is_open() == false) {
            return false;
        } else {
            return true;
        }
        return true;
    } else if (operationType_ == kDirectIO) {
        return true;
    } else {
        return false;
    }
}

bool FileOperation::open(string path)
{
    if (operationType_ == kFstream) {
        fileStream_.open(path, ios::in | ios::out | ios::binary);
        if (fileStream_.is_open() == false) {
            return false;
        } else {
            return true;
        }
    } else if (operationType_ == kDirectIO) {
        return true;
    } else {
        return false;
    }
}

bool FileOperation::close()
{
    if (operationType_ == kFstream) {
        fileStream_.flush();
        fileStream_.close();
        return true;
    } else if (operationType_ == kDirectIO) {
        return true;
    } else {
        return false;
    }
}

bool FileOperation::write(char* contentBuffer, uint64_t contentSize)
{
    if (operationType_ == kFstream) {
        fileStream_.write(contentBuffer, contentSize);
        return true;
    } else if (operationType_ == kDirectIO) {
        return true;
    } else {
        return false;
    }
}

bool FileOperation::read(char* contentBuffer, uint64_t contentSize)
{
    if (operationType_ == kFstream) {
        fileStream_.read(contentBuffer, contentSize);
        return true;
    } else if (operationType_ == kDirectIO) {
        return true;
    } else {
        return false;
    }
}

bool FileOperation::flush()
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

bool FileOperation::resetPointer(fileOperationSetPointerOps ops, uint64_t offset)
{
    if (operationType_ == kFstream) {
        if (ops == kBegin) {
            fileStream_.seekg(offset, ios::beg);
            fileStream_.seekp(offset, ios::beg);
        } else {
            fileStream_.seekg(offset, ios::end);
            fileStream_.seekp(offset, ios::end);
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
        return fileSize;
    } else if (operationType_ == kDirectIO) {
        uint64_t fileRealSizeWithoutPadding;
        return fileRealSizeWithoutPadding;
    } else {
        return 0;
    }
}

} // namespace DELTAKV_NAMESPACE