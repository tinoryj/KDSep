#include "utils/mempool.hpp"

using namespace std;

namespace DELTAKV_NAMESPACE {

KeyValueMemPool::KeyValueMemPool(uint32_t objectNumberThreshold, uint32_t maxBlockSize)
{
    mempool_ = (uint8_t**)malloc(objectNumberThreshold * sizeof(uint8_t*));
    for (uint32_t i = 0; i < objectNumberThreshold; i++) {
        mempool_[i] = (uint8_t*)malloc(maxBlockSize * sizeof(uint8_t));
        mempoolFreeHandlerQueue_.push_back(i);
    }
    mempoolBlockNumberThreshold_ = objectNumberThreshold;
}

KeyValueMemPool::~KeyValueMemPool()
{
    mempoolFreeHandlerQueue_.clear();
    for (uint32_t i = 0; i < mempoolBlockNumberThreshold_; i++) {
        free(mempool_[i]);
    }
    free(mempool_);
}

bool KeyValueMemPool::insertContentToMemPoolAndGetHandler(string keyStr, string valueStr, uint32_t sequenceNumber, bool isAnchorFlag, mempoolHandler_t& mempoolHandler)
{
    if (mempoolFreeHandlerQueue_.size() == 0) {
        return false;
    } else {
        managerMtx_.lock();
        mempoolHandler.mempoolHandlerID_ = mempoolFreeHandlerQueue_.front();
        mempoolFreeHandlerQueue_.pop_front();
        managerMtx_.unlock();
        mempoolHandler.keySize_ = keyStr.size();
        mempoolHandler.valueSize_ = valueStr.size();
        memcpy(mempool_[mempoolHandler.mempoolHandlerID_], keyStr.c_str(), keyStr.size());
        memcpy(mempool_[mempoolHandler.mempoolHandlerID_] + mempoolHandler.keySize_, valueStr.c_str(), valueStr.size());
        mempoolHandler.keyPtr_ = mempool_[mempoolHandler.mempoolHandlerID_];
        mempoolHandler.valuePtr_ = mempool_[mempoolHandler.mempoolHandlerID_] + mempoolHandler.keySize_;
        mempoolHandler.sequenceNumber_ = sequenceNumber;
        mempoolHandler.isAnchorFlag_ = isAnchorFlag;
        return true;
    }
}

bool KeyValueMemPool::eraseContentFromMemPool(mempoolHandler_t mempoolHandler)
{
    managerMtx_.lock();
    mempoolFreeHandlerQueue_.push_back(mempoolHandler.mempoolHandlerID_);
    managerMtx_.unlock();
    return true;
}

}