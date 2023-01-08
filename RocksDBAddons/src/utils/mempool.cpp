#include "utils/mempool.hpp"

using namespace std;

namespace DELTAKV_NAMESPACE {

KeyValueMemPool::KeyValueMemPool(uint32_t objectNumberThreshold, uint32_t maxBlockSize)
{
    mempool_ = (char**)malloc(objectNumberThreshold * sizeof(char*));
    for (uint32_t i = 0; i < objectNumberThreshold; i++) {
        mempool_[i] = (char*)malloc(maxBlockSize * sizeof(char));
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
    std::scoped_lock<std::shared_mutex> wlock(managerMtx_);
    if (mempoolFreeHandlerQueue_.size() == 0) {
        return false;
    } else {
        mempoolHandler.mempoolHandlerID_ = mempoolFreeHandlerQueue_.front();
        mempoolFreeHandlerQueue_.pop_front();
        if (mempoolHandler.mempoolHandlerID_ > mempoolBlockNumberThreshold_ - 1) {
            debug_error("[ERROR] Get overflowed mempool handler, ID = %u, threshold = %u\n", mempoolHandler.mempoolHandlerID_, mempoolBlockNumberThreshold_);
        }
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
    std::scoped_lock<std::shared_mutex> wlock(managerMtx_);
    mempoolFreeHandlerQueue_.push_back(mempoolHandler.mempoolHandlerID_);
    return true;
}

}