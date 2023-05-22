#pragma once

#include "utils/debug.hpp"
#include <bits/stdc++.h>

using namespace std;

namespace DELTAKV_NAMESPACE {

typedef struct mempoolHandler_t {
    uint32_t mempoolHandlerID_;
    uint32_t keySize_;
    uint32_t valueSize_;
    uint32_t sequenceNumber_;
    bool isAnchorFlag_;
    char* keyPtr_ = nullptr;
    char* valuePtr_ = nullptr;
    mempoolHandler_t(uint32_t mempoolHandlerID,
        uint32_t keySize,
        uint32_t valueSize,
        char* keyPtr,
        char* valuePtr)
    {
        mempoolHandlerID_ = mempoolHandlerID;
        keySize_ = keySize;
        valueSize_ = valueSize;
        keyPtr_ = keyPtr;
        valuePtr_ = valuePtr;
    }
    mempoolHandler_t() { }
} mempoolHandler_t;

class KeyValueMemPool {

public:
    KeyValueMemPool(uint32_t objectNumberThreshold, uint32_t maxBlockSize);
    ~KeyValueMemPool();
    bool insertContentToMemPoolAndGetHandler(string keyStr, string valueStr, uint32_t sequenceNumber, bool isAnchorFlag, mempoolHandler_t& mempoolHandler);
    bool eraseContentFromMemPool(mempoolHandler_t mempoolHandler);

private:
    char** mempool_;
    uint32_t mempoolBlockNumberThreshold_;
    uint32_t mempoolBlockSizeThreshold_;
    uint32_t* mempoolFreeHandlerVec_;
    uint32_t mempoolFreeHandlerVecStartPtr_;
    uint32_t mempoolFreeHandlerVecEndPtr_;
    std::shared_mutex managerMtx_;
};

}