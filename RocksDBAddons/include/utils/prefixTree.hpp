#pragma once

#include "utils/debug.hpp"
#include "utils/statsRecorder.hh"
#include <bits/stdc++.h>
#include <shared_mutex>
#include <stack>
using namespace std;

namespace DELTAKV_NAMESPACE {

template <typename dataT>
class PrefixTree {

public:
    PrefixTree(uint64_t initBitNumber, uint64_t maxBitNumber)
    {
        initBitNumber_ = initBitNumber;
        maxBitNumber_ = maxBitNumber;
        rootNode_ = new prefixTreeNode;
        {
            std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
            createPrefixTree(rootNode_, 0);
        }
    }

    PrefixTree()
    {
        rootNode_ = new prefixTreeNode;
    }

    ~PrefixTree()
    {
        stack<prefixTreeNode*> stk;
        prefixTreeNode *p = rootNode_, *pre = nullptr;

        // almost a template for post order traversal ...
        while (p != nullptr || !stk.empty()) {
            while (p != nullptr) {
                stk.push(p);
                p = p->leftChildNodePtr_; // go down one level
            }

            if (!stk.empty()) {
                p = stk.top(); // its left children are deleted
                stk.pop();
                if (p->rightChildNodePtr_ == nullptr || pre == p->rightChildNodePtr_) {
                    debug_trace("delete p %s\n", p->currentNodePrefix.c_str());
                    delete p;
                    pre = p;
                    p = nullptr;
                } else {
                    stk.push(p);
                    p = p->rightChildNodePtr_;
                }
            }
        }
    }

    void init(uint64_t initBitNumber, uint64_t maxBitNumber)
    {
        initBitNumber_ = initBitNumber;
        maxBitNumber_ = maxBitNumber;
        {
            std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
            createPrefixTree(rootNode_, 0);
        }
    }

    uint64_t insert(string prefixStr, dataT& newData)
    {
        uint64_t insertAtLevel = 0;
        bool status;
        {
            std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
            status = addPrefixTreeNode(rootNode_, prefixStr, newData, insertAtLevel);
        }
        if (status == true) {
            debug_trace("Insert to new node success at level = %lu, for prefix = %s\n", insertAtLevel, prefixStr.c_str());
            return insertAtLevel;
        } else {
            debug_error("[ERROR] Insert to new node fail at level = %lu, for prefix = %s\n", insertAtLevel, prefixStr.c_str());
            printNodeMap();
            return 0;
        }
    }

    uint64_t insertWithFixedBitNumber(string prefixStr, uint64_t fixedBitNumber, dataT& newData)
    {
        uint64_t insertAtLevel = 0;
        bool status;
        {
            std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
            status = addPrefixTreeNodeWithFixedBitNumber(rootNode_, prefixStr, fixedBitNumber, newData, insertAtLevel);
        }
        if (status == true) {
            debug_trace("Insert to new node with fixed bit number =  %lu, success at level =  %lu, for prefix = %s\n", fixedBitNumber, insertAtLevel, prefixStr.c_str());
            return insertAtLevel;
        } else {
            debug_error("[ERROR] Insert to new node with fixed bit number =  %lu, fail at level =  %lu, for prefix = %s\n", fixedBitNumber, insertAtLevel, prefixStr.c_str());
            printNodeMap();
            return 0;
        }
    }

    bool get(string prefixStr, dataT& newData)
    {
        uint64_t findAtLevelID = 0;
        bool status;
        {
            std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
            status = findPrefixTreeNode(rootNode_, prefixStr, newData, findAtLevelID);
        }
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    bool find(string prefixStr, uint64_t& findAtLevelID)
    {
        dataT newData;
        bool status;
        {
            std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
            status = findPrefixTreeNode(rootNode_, prefixStr, newData, findAtLevelID);
        }
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    bool remove(string prefixStr, uint64_t& findAtLevelID)
    {
        bool status;
        {
            std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
            status = removePrefixTreeNode(rootNode_, prefixStr, findAtLevelID);
        }
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    bool getCurrentValidNodes(vector<pair<string, dataT>>& validObjectList)
    {
        {
            std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

            prefixTreeNode *p = rootNode_, *pre = nullptr;
            stack<prefixTreeNode*> stk;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->leftChildNodePtr_;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->rightChildNodePtr_ == nullptr || pre == p->rightChildNodePtr_) {
                        if (p->isLeafNodeFlag_ == true) {
                            validObjectList.push_back(make_pair(p->currentNodePrefix, p->data_));
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->rightChildNodePtr_;
                    }
                }
            }
        }
        if (validObjectList.size() != 0) {
            return true;
        } else {
            return false;
        }
    }

    bool getPossibleValidNodes(vector<pair<string, dataT>>& validObjectList)
    {
        {
            std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

            // post order
            stack<prefixTreeNode*> stk;
            prefixTreeNode *p = rootNode_, *pre = nullptr;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->leftChildNodePtr_;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->rightChildNodePtr_ == nullptr || pre == p->rightChildNodePtr_) {
                        if (p->currentNodePrefix.size() != 0) {
                            validObjectList.push_back(make_pair(p->currentNodePrefix, p->data_));
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->rightChildNodePtr_;
                    }
                }
            }
        }
        if (validObjectList.size() != 0) {
            return true;
        } else {
            return false;
        }
    }

    bool getInValidNodes(vector<pair<string, dataT>>& invalidObjectList)
    {
        {
            std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

            stack<prefixTreeNode*> stk;
            prefixTreeNode *p = rootNode_, *pre = nullptr;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->leftChildNodePtr_;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->rightChildNodePtr_ == nullptr || pre == p->rightChildNodePtr_) {
                        if (p->currentNodePrefix.size() != 0 && p->isLeafNodeFlag_ == false) {
                            invalidObjectList.push_back(make_pair(p->currentNodePrefix, p->data_));
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->rightChildNodePtr_;
                    }
                }
            }
        }
        if (invalidObjectList.size() != 0) {
            return true;
        } else {
            return false;
        }
    }

    void printNodeMap()
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

        stack<prefixTreeNode*> stk;
        prefixTreeNode *p = rootNode_, *pre = nullptr;
        while (!stk.empty() || p != nullptr) {
            while (p != nullptr) {
                stk.push(p);
                p = p->leftChildNodePtr_;
            }

            if (!stk.empty()) {
                p = stk.top();
                stk.pop();
                if (p->rightChildNodePtr_ == nullptr || pre == p->rightChildNodePtr_) {
                    if (p->currentNodePrefix.size() != 0) {
                        debug_trace("Find node, is leaf node flag = %d, prefix length = %lu, linked prefix = %s\n", p->isLeafNodeFlag_, p->currentNodePrefix.size(), p->currentNodePrefix.c_str());
                    }
                    pre = p;
                    p = nullptr;
                } else {
                    stk.push(p);
                    p = p->rightChildNodePtr_;
                }
            }
        }
    }

private:
    typedef struct prefixTreeNode {
        prefixTreeNode* previousNodePtr_ = nullptr;
        uint64_t thisNodeID_ = 0;
        prefixTreeNode* leftChildNodePtr_ = nullptr; // 0
        prefixTreeNode* rightChildNodePtr_ = nullptr; // 1
        bool isLeafNodeFlag_ = false;
        string currentNodePrefix;
        dataT data_;
    } prefixTreeNode;

    std::shared_mutex nodeOperationMtx_;
    uint64_t nextNodeID_ = 0;
    uint64_t initBitNumber_;
    uint64_t maxBitNumber_;
    prefixTreeNode* rootNode_;

    void createPrefixTree(prefixTreeNode* root, uint64_t currentLevel)
    {
        currentLevel++;
        root->isLeafNodeFlag_ = false;
        root->thisNodeID_ = nextNodeID_;
        nextNodeID_++;
        if (currentLevel != initBitNumber_) {
            root->leftChildNodePtr_ = new prefixTreeNode;
            root->rightChildNodePtr_ = new prefixTreeNode;
            createPrefixTree(root->leftChildNodePtr_, currentLevel);
            createPrefixTree(root->rightChildNodePtr_, currentLevel);
        } else {
            return;
        }
    }

    bool addPrefixTreeNode(prefixTreeNode* root, string bitBasedPrefixStr, dataT newDataObj, uint64_t& insertAtLevelID)
    {
        uint64_t currentLevel = 0;
        for (; currentLevel < bitBasedPrefixStr.size() && currentLevel < maxBitNumber_; currentLevel++) {
            // cout << "Current level = " << currentLevel << endl;
            if (bitBasedPrefixStr.at(currentLevel) == '0') {
                // go to left if 0
                if (root->leftChildNodePtr_ == nullptr) {
                    root->leftChildNodePtr_ = new prefixTreeNode;
                    // insert at next level
                    root = root->leftChildNodePtr_;
                    root->isLeafNodeFlag_ = true;
                    root->data_ = newDataObj;
                    root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel + 1);
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                    insertAtLevelID = currentLevel + 1;
                    return true;
                } else {
                    root = root->leftChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        debug_warn("Meet old leaf node (left) during add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                        break;
                    } else {
                        continue;
                    }
                }
            } else {
                // go to right if 1
                if (root->rightChildNodePtr_ == nullptr) {
                    root->rightChildNodePtr_ = new prefixTreeNode;
                    // insert at next level
                    root = root->rightChildNodePtr_;
                    root->isLeafNodeFlag_ = true;
                    root->data_ = newDataObj;
                    root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel + 1);
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                    insertAtLevelID = currentLevel + 1;
                    return true;
                } else {
                    root = root->rightChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        debug_warn("Meet old leaf node (right) during add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                        break;
                    } else {
                        continue;
                    }
                }
            }
        }
        currentLevel++;
        if (currentLevel >= maxBitNumber_) {
            debug_error("[ERROR] Reached max bit number during add, could not add new node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
            return false;
        }
        if (bitBasedPrefixStr.at(currentLevel) == '0') {
            // go to left if 0
            if (root->leftChildNodePtr_ == nullptr) {
                root->leftChildNodePtr_ = new prefixTreeNode;
                // insert at next level
                root = root->leftChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel + 1);
                root->thisNodeID_ = nextNodeID_;
                nextNodeID_++;
                insertAtLevelID = currentLevel + 1;
                return true;
            } else {
                debug_error("[ERROR] Find left node after leaf node mark, error, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                return false;
            }
        } else {
            // go to right if 1
            if (root->rightChildNodePtr_ == nullptr) {
                root->rightChildNodePtr_ = new prefixTreeNode;
                // insert at next level
                root = root->rightChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel + 1);
                root->thisNodeID_ = nextNodeID_;
                nextNodeID_++;
                insertAtLevelID = currentLevel + 1;
                return true;
            } else {
                debug_error("[ERROR] Find right node after leaf node mark, error, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                return false;
            }
        }
        return false;
    }

    bool addPrefixTreeNodeWithFixedBitNumber(prefixTreeNode* root, string bitBasedPrefixStr, uint64_t fixedBitNumber, dataT newDataObj, uint64_t& insertAtLevelID)
    {
        uint64_t currentLevel = 0;
        for (; currentLevel < fixedBitNumber - 1; currentLevel++) {
            // cout << "Current level = " << currentLevel << endl;
            if (bitBasedPrefixStr.at(currentLevel) == '0') {
                // go to left if 0
                if (root->leftChildNodePtr_ == nullptr) {
                    root->leftChildNodePtr_ = new prefixTreeNode;
                    root = root->leftChildNodePtr_;
                    root->isLeafNodeFlag_ = false;
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                } else {
                    root = root->leftChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        debug_warn("Meet old leaf node (left) during fixed bit number add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                        continue;
                    } else {
                        continue;
                    }
                }
            } else {
                // go to right if 1
                if (root->rightChildNodePtr_ == nullptr) {
                    root->rightChildNodePtr_ = new prefixTreeNode;
                    root = root->rightChildNodePtr_;
                    root->isLeafNodeFlag_ = false;
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                } else {
                    root = root->rightChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        debug_warn("Meet old leaf node (right) during fixed bit number add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                        continue;
                    } else {
                        continue;
                    }
                }
            }
        }
        currentLevel++;
        if (currentLevel >= maxBitNumber_) {
            debug_error("[ERROR] Reached max bit number during fixed bit number add, could not add new node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
            return false;
        }
        if (bitBasedPrefixStr.at(fixedBitNumber - 1) == '0') {
            // go to left if 0
            if (root->leftChildNodePtr_ == nullptr) {
                root->leftChildNodePtr_ = new prefixTreeNode;
                root = root->leftChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel);
                root->thisNodeID_ = nextNodeID_;
                nextNodeID_++;
                insertAtLevelID = currentLevel;
                return true;
            } else {
                debug_error("[ERROR] Find left node after leaf node mark, error during fixed bit number add, could not add new node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                return false;
            }
        } else {
            // go to right if 1
            if (root->rightChildNodePtr_ == nullptr) {
                root->rightChildNodePtr_ = new prefixTreeNode;
                root = root->rightChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel);
                root->thisNodeID_ = nextNodeID_;
                nextNodeID_++;
                insertAtLevelID = currentLevel;
                return true;
            } else {
                debug_error("[ERROR] Find right node after leaf node mark, error during fixed bit number add, could not add new node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                return false;
            }
        }
        return false;
    }

    bool findPrefixTreeNode(prefixTreeNode* root, string bitBasedPrefixStr, dataT& currentDataTObj, uint64_t& findAtLevelID)
    {
        uint64_t currentLevel = 0;
        for (; currentLevel < bitBasedPrefixStr.size() && currentLevel < maxBitNumber_; currentLevel++) {
            if (bitBasedPrefixStr.at(currentLevel) == '0') {
                // go to left if 0
                if (root->isLeafNodeFlag_ == true) {
                    currentDataTObj = root->data_;
                    findAtLevelID = currentLevel;
                    return true;
                } else {
                    if (root->leftChildNodePtr_ == nullptr) {
                        debug_info("No left node, but this node is not leaf node, not exist. current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                        return false;
                    } else {
                        root = root->leftChildNodePtr_;
                    }
                }
            } else {
                // go to right if 1
                if (root->isLeafNodeFlag_ == true) {
                    currentDataTObj = root->data_;
                    findAtLevelID = currentLevel;
                    return true;
                } else {
                    if (root->rightChildNodePtr_ == nullptr) {
                        debug_info("No right node, but this node is not leaf node, not exist. current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                        return false;
                    } else {
                        root = root->rightChildNodePtr_;
                    }
                }
            }
        }
        if (root == nullptr) {
            debug_info("This node not exist, may be deleted. current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
            return false;
        } else {
            if (root->isLeafNodeFlag_ == true) {
                currentDataTObj = root->data_;
                findAtLevelID = currentLevel;
                return true;
            } else {
                debug_info("This node is not leaf node. current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                return false;
            }
        }
    }

    bool removePrefixTreeNode(prefixTreeNode* root, string bitBasedPrefixStr, uint64_t& findAtLevelID)
    {
        uint64_t searchLevelNumber = bitBasedPrefixStr.size();
        findAtLevelID = 0;
        for (uint64_t currentLevel = 0; currentLevel < searchLevelNumber; currentLevel++) {
            if (bitBasedPrefixStr.at(currentLevel) == '0') {
                // go to left if 0
                root = root->leftChildNodePtr_;
            } else {
                // go to right if 1
                root = root->rightChildNodePtr_;
            }
            findAtLevelID++;
        }
        if (root != nullptr && root->isLeafNodeFlag_ == true) {
            debug_trace("Find leaf node ID = %lu, node prefix length = %lu, prefix = %s remove it now\n", root->thisNodeID_, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
            root->isLeafNodeFlag_ = false;
            return true;
        } else {
            if (root != nullptr) {
                debug_error("[ERROR] Could not delete target node (not leaf) ID = %lu, node prefix length = %lu, prefix = %s remove it now\n", root->thisNodeID_, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
            } else {
                debug_error("[ERROR] Could not delete target node (not exist) pointer = %p\n", (void*)root);
            }
            return false;
        }
    }
};

}
