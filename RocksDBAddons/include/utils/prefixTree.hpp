#pragma once

#include <bits/stdc++.h>
using namespace std;

namespace DELTAKV_NAMESPACE {

template <typename dataT>
class PrefixTree {

public:
    PrefixTree(uint64_t initBitNumber, uint64_t maxBitNumber)
    {
        initBitNumber_ = initBitNumber;
        maxBitNumber_ = maxBitNumber;
        rootNode_.previousNodeID_ = 0;
        rootNode_.thisNodeID_ = 0;
        for (auto index = 0; index < ini)
    }

    ~PrefixTree();
    bool insert(string prefixStr, dataT& newData)
    {
    }
    bool find(string prefixStr);
    bool get(string prefixStr, dataT& newData);

private:
    typedef struct prefixTreeNode {
        uint64_t previousNodeID_;
        uint64_t thisNodeID_;
        uint64_t leftChildNodeID_; // ID = 1
        uint64_t rightChildNodeID_; // ID = 0
        bool isLeafNodeFlag_;
        dataT data_;
    } prefixTreeNode;
    unordered_map<uint64_t, prefixTreeNode> nodeMap_;
    shared_mutex nodeOperationMtx_;
    uint64_t nextNodeID_ = 0;
    uint64_t initBitNumber_;
    uint64_t maxBitNumber_;
    prefixTreeNode rootNode_;
    unordered_map<uint64_t, dataT>;
};

}