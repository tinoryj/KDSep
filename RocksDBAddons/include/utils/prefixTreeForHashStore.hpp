#pragma once

#include "common/dataStructure.hpp"
#include "utils/debug.hpp"
#include "utils/statsRecorder.hh"
#include <bits/stdc++.h>
#include <shared_mutex>
#include <stack>
using namespace std;

namespace DELTAKV_NAMESPACE {

class PrefixTreeForHashStore {

public:
    PrefixTreeForHashStore(uint64_t initBitNumber, uint64_t maxFileNumber)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        initBitNumber_ = initBitNumber;
        maxFileNumber_ = maxFileNumber;
        rootNode_ = new prefixTreeNode;
        createPrefixTree(rootNode_, 0);
    }

    PrefixTreeForHashStore()
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        rootNode_ = new prefixTreeNode;
    }

    ~PrefixTreeForHashStore()
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
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
        for (long unsigned int i = 0; i < targetDeleteVec.size(); i++) {
            if (targetDeleteVec[i] != nullptr) {
                if (targetDeleteVec[i]->file_operation_func_ptr_ != nullptr) {
                    delete targetDeleteVec[i]->file_operation_func_ptr_;
                }
                delete targetDeleteVec[i];
            }
        }
    }

    void init(uint64_t initBitNumber, uint64_t maxFileNumber)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        initBitNumber_ = initBitNumber;
        maxFileNumber_ = maxFileNumber;
        createPrefixTree(rootNode_, 0);
    }

    uint64_t getRemainFileNumber()
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        return maxFileNumber_ - currentFileNumber_;
    }

    uint64_t insert(const string& prefixStr, hashStoreFileMetaDataHandler*& newData)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        if (currentFileNumber_ >= maxFileNumber_) {
            debug_error("[ERROR] Could note insert new node, since there are too many files, number = %lu, threshold = %lu\n", currentFileNumber_, maxFileNumber_);
            printNodeMap();
            return 0;
        }
        uint64_t insertAtLevel = 0;
        bool status = addPrefixTreeNode(rootNode_, prefixStr, newData, insertAtLevel);
        if (status == true) {
            currentFileNumber_++;
            debug_trace("Insert to new node success at level = %lu, for prefix = %s, current file number = %lu\n", insertAtLevel, prefixStr.c_str(), currentFileNumber_);
            return insertAtLevel;
        } else {
            debug_error("[ERROR] Insert to new node fail at level = %lu, for prefix = %s\n", insertAtLevel, prefixStr.c_str());
            printNodeMap();
            return 0;
        }
    }

    uint64_t insert(const uint64_t& prefixU64, hashStoreFileMetaDataHandler*& newData)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        if (currentFileNumber_ >= maxFileNumber_) {
            debug_error("[ERROR] Could note insert new node, since there are too many files, number = %lu, threshold = %lu\n", currentFileNumber_, maxFileNumber_);
            printNodeMap();
            return 0;
        }
        uint64_t insertAtLevel = 0;
        bool status = addPrefixTreeNode(rootNode_, prefixU64, newData, insertAtLevel);
        if (status == true) {
            currentFileNumber_++;
            debug_trace("Insert to new node success at level = %lu, for prefix = %lx, current file number = %lu\n", insertAtLevel, prefixU64, currentFileNumber_);
            return insertAtLevel;
        } else {
            debug_error("[ERROR] Insert to new node fail at level = %lu, for prefix = %lx\n", insertAtLevel, prefixU64);
            printNodeMap();
            return 0;
        }
    }

    pair<uint64_t, uint64_t> insertPairOfNodes(const string& prefixStr1, hashStoreFileMetaDataHandler*& newData1, const string& prefixStr2, hashStoreFileMetaDataHandler*& newData2)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        if (currentFileNumber_ >= (maxFileNumber_ + 1)) {
            debug_error("[ERROR] Could note insert new node, since there are too many files, number = %lu, threshold = %lu\n", currentFileNumber_, maxFileNumber_);
            printNodeMap();
            return make_pair(0, 0);
        }
        uint64_t insertAtLevel1 = 0;
        uint64_t insertAtLevel2 = 0;
        bool status = addPrefixTreeNode(rootNode_, prefixStr1, newData1, insertAtLevel1);
        if (status == true) {
            currentFileNumber_++;
            debug_trace("Insert to first new node success at level = %lu, for prefix = %s, current file number = %lu\n", insertAtLevel1, prefixStr1.c_str(), currentFileNumber_);
            // add another node
            status = addPrefixTreeNode(rootNode_, prefixStr2, newData2, insertAtLevel2);
            if (status == true) {
                currentFileNumber_++;
                debug_trace("Insert to second new node success at level = %lu, for prefix = %s, current file number = %lu\n", insertAtLevel2, prefixStr2.c_str(), currentFileNumber_);
                return make_pair(insertAtLevel1, insertAtLevel2);
            } else {
                debug_error("[ERROR] Insert to second new node fail at level = %lu, for prefix = %s\n", insertAtLevel1, prefixStr1.c_str());
                printNodeMap();
                return make_pair(insertAtLevel1, insertAtLevel2);
            }
        } else {
            debug_error("[ERROR] Insert to first new node fail at level = %lu, for prefix = %s\n", insertAtLevel1, prefixStr1.c_str());
            printNodeMap();
            return make_pair(insertAtLevel1, insertAtLevel2);
        }
    }

    uint64_t insertWithFixedBitNumber(const string& prefixStr, uint64_t fixedBitNumber, hashStoreFileMetaDataHandler*& newData)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        debug_info("Current file number = %lu, threshold = %lu\n", currentFileNumber_, maxFileNumber_);
        if (currentFileNumber_ >= maxFileNumber_) {
            debug_error("[ERROR] Could note insert new node, since there are too many files, number = %lu, threshold = %lu\n", currentFileNumber_, maxFileNumber_);
            printNodeMap();
            return 0;
        }
        uint64_t insertAtLevel = 0;
        bool status = addPrefixTreeNodeWithFixedBitNumber(rootNode_, prefixStr, fixedBitNumber, newData, insertAtLevel);
        if (status == true) {
            debug_trace("Insert to new node with fixed bit number =  %lu, success at level =  %lu, for prefix = %s\n", fixedBitNumber, insertAtLevel, prefixStr.c_str());
            currentFileNumber_++;
            return insertAtLevel;
        } else {
            debug_error("[ERROR] Insert to new node with fixed bit number =  %lu, fail at level =  %lu, for prefix = %s\n", fixedBitNumber, insertAtLevel, prefixStr.c_str());
            printNodeMap();
            return 0;
        }
    }

    bool get(const string& prefixStr, hashStoreFileMetaDataHandler*& newData)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        uint64_t findAtLevelID = 0;
        bool status = findPrefixTreeNode(rootNode_, prefixStr, newData, findAtLevelID);
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    bool get(const uint64_t& prefixU64, hashStoreFileMetaDataHandler*& newData)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        uint64_t findAtLevelID = 0;
        bool status = findPrefixTreeNode(rootNode_, prefixU64, newData, findAtLevelID);
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    bool find(const string& prefixStr, uint64_t& findAtLevelID)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        hashStoreFileMetaDataHandler* newData;
        bool status = findPrefixTreeNode(rootNode_, prefixStr, newData, findAtLevelID);
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    bool remove(const string& prefixStr, uint64_t& findAtLevelID)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        bool status = markPrefixTreeNodeAsNonLeafNode(rootNode_, prefixStr, findAtLevelID);
        if (status == true) {
            currentFileNumber_--;
            return true;
        } else {
            return false;
        }
    }

    bool mergeNodesToNewLeafNode(const string& prefixStr, uint64_t& findAtLevelID)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        bool status = markPrefixTreeNodeAsNewLeafNodeAndDeleteChildren(rootNode_, prefixStr, findAtLevelID);
        if (status == true) {
            currentFileNumber_--;
            return true;
        } else {
            return false;
        }
    }

    bool updateDataObjectForTargetLeafNode(const string& bitBasedPrefixStr, uint64_t& findAtLevelID, hashStoreFileMetaDataHandler* newDataObj)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        bool status = updateLeafNodeDataObject(rootNode_, bitBasedPrefixStr, findAtLevelID, newDataObj);
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    bool getCurrentValidNodes(vector<pair<string, hashStoreFileMetaDataHandler*>>& validObjectList)
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

        if (validObjectList.size() != 0) {
            return true;
        } else {
            return false;
        }
    }

    bool getPossibleValidNodes(vector<pair<string, hashStoreFileMetaDataHandler*>>& validObjectList)
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
        if (validObjectList.size() != 0) {
            return true;
        } else {
            return false;
        }
    }

    bool getInValidNodes(vector<pair<string, hashStoreFileMetaDataHandler*>>& invalidObjectList)
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
        if (invalidObjectList.size() != 0) {
            return true;
        } else {
            return false;
        }
    }

    void printNodeMap()
    {
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
        hashStoreFileMetaDataHandler* data_ = nullptr; //
    } prefixTreeNode;
    vector<hashStoreFileMetaDataHandler*> targetDeleteVec;
    std::shared_mutex nodeOperationMtx_;
    uint64_t nextNodeID_ = 0;
    uint64_t initBitNumber_ = 0;
    uint64_t maxFileNumber_ = 0;
    uint64_t currentFileNumber_ = 0;
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

    bool addPrefixTreeNode(prefixTreeNode* root, const string& bitBasedPrefixStr, hashStoreFileMetaDataHandler* newDataObj, uint64_t& insertAtLevelID)
    {
        uint64_t currentLevel = 0;
        for (; currentLevel < bitBasedPrefixStr.size(); currentLevel++) {
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
                        currentFileNumber_--;
                        debug_info("Meet old leaf node (left) during add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s, currentFilnumber = %lu\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str(), currentFileNumber_);
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
                        currentFileNumber_--;
                        debug_info("Meet old leaf node (right) during add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s, currentFilnumber = %lu\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str(), currentFileNumber_);
                        break;
                    } else {
                        continue;
                    }
                }
            }
        }
        currentLevel++;
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

    bool addPrefixTreeNode(prefixTreeNode* root, const uint64_t& prefixU64, hashStoreFileMetaDataHandler* newDataObj, uint64_t& insertAtLevelID)
    {
        uint64_t currentLevel = 0;
        char prefixStr[64]; 
        for (; currentLevel < 64; currentLevel++) {
            // cout << "Current level = " << currentLevel << endl;
            if ((prefixU64 & (1 << currentLevel)) == 0) {
                prefixStr[currentLevel] = '0';
                // go to left if 0
                if (root->leftChildNodePtr_ == nullptr) {
                    root->leftChildNodePtr_ = new prefixTreeNode;
                    // insert at next level
                    root = root->leftChildNodePtr_;
                    root->isLeafNodeFlag_ = true;
                    root->data_ = newDataObj;
                    root->currentNodePrefix = string(prefixStr, currentLevel + 1);
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                    insertAtLevelID = currentLevel + 1;
                    return true;
                } else {
                    root = root->leftChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        currentFileNumber_--;
                        debug_info("Meet old leaf node (left) during add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s, currentFilnumber = %lu\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str(), currentFileNumber_);
                        break;
                    } else {
                        continue;
                    }
                }
            } else {
                prefixStr[currentLevel] = '1';
                // go to right if 1
                if (root->rightChildNodePtr_ == nullptr) {
                    root->rightChildNodePtr_ = new prefixTreeNode;
                    // insert at next level
                    root = root->rightChildNodePtr_;
                    root->isLeafNodeFlag_ = true;
                    root->data_ = newDataObj;
                    root->currentNodePrefix = string(prefixStr, currentLevel + 1);
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                    insertAtLevelID = currentLevel + 1;
                    return true;
                } else {
                    root = root->rightChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        currentFileNumber_--;
                        debug_info("Meet old leaf node (right) during add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s, currentFilnumber = %lu\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str(), currentFileNumber_);
                        break;
                    } else {
                        continue;
                    }
                }
            }
        }
        currentLevel++;
        if ((prefixU64 & (1 << currentLevel)) == 0) {
            prefixStr[currentLevel] = '0';
            // go to left if 0
            if (root->leftChildNodePtr_ == nullptr) {
                root->leftChildNodePtr_ = new prefixTreeNode;
                // insert at next level
                root = root->leftChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = string(prefixStr, currentLevel + 1);
                root->thisNodeID_ = nextNodeID_;
                nextNodeID_++;
                insertAtLevelID = currentLevel + 1;
                return true;
            } else {
                debug_error("[ERROR] Find left node after leaf node mark, error, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                return false;
            }
        } else {
            prefixStr[currentLevel] = '1';
            // go to right if 1
            if (root->rightChildNodePtr_ == nullptr) {
                root->rightChildNodePtr_ = new prefixTreeNode;
                // insert at next level
                root = root->rightChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = string(prefixStr, currentLevel + 1);
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

    bool addPrefixTreeNodeWithFixedBitNumber(prefixTreeNode* root, const string& bitBasedPrefixStr, uint64_t fixedBitNumber, hashStoreFileMetaDataHandler* newDataObj, uint64_t& insertAtLevelID)
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
                        debug_info("Meet old leaf node (left) during fixed bit number add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
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
                        debug_info("Meet old leaf node (right) during fixed bit number add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s\n", currentLevel, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
                        continue;
                    } else {
                        continue;
                    }
                }
            }
        }
        currentLevel++;
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

    bool findPrefixTreeNode(prefixTreeNode* root, const string& bitBasedPrefixStr, hashStoreFileMetaDataHandler*& currentDataTObj, uint64_t& findAtLevelID)
    {
        uint64_t currentLevel = 0;
        for (; currentLevel < bitBasedPrefixStr.size(); currentLevel++) {
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

    bool findPrefixTreeNode(prefixTreeNode* root, const uint64_t& prefixU64, hashStoreFileMetaDataHandler*& currentDataTObj, uint64_t& findAtLevelID)
    {
        uint64_t currentLevel = 0;
        for (; currentLevel < 64; currentLevel++) {
            if ((prefixU64 & (1 << currentLevel)) == 0) {
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

    bool markPrefixTreeNodeAsNonLeafNode(prefixTreeNode* root, const string& bitBasedPrefixStr, uint64_t& findAtLevelID)
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

    bool markPrefixTreeNodeAsNewLeafNodeAndDeleteChildren(prefixTreeNode* root, const string& bitBasedPrefixStr, uint64_t& findAtLevelID)
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
        if (root != nullptr && root->isLeafNodeFlag_ == false) {
            debug_trace("Find non leaf node ID = %lu, node prefix length = %lu, prefix = %s mark it as leaf now\n", root->thisNodeID_, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
            root->isLeafNodeFlag_ = true;
            root->currentNodePrefix = bitBasedPrefixStr;
            if (root->leftChildNodePtr_->data_ != nullptr) {
                targetDeleteVec.push_back(root->leftChildNodePtr_->data_);
            }
            if (root->rightChildNodePtr_->data_ != nullptr) {
                targetDeleteVec.push_back(root->rightChildNodePtr_->data_);
            }
            delete root->leftChildNodePtr_;
            delete root->rightChildNodePtr_;
            root->leftChildNodePtr_ = nullptr;
            root->rightChildNodePtr_ = nullptr;
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

    bool updateLeafNodeDataObject(prefixTreeNode* root, const string& bitBasedPrefixStr, uint64_t& findAtLevelID, hashStoreFileMetaDataHandler* newDataObj)
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
            debug_trace("Find target leaf node ID = %lu, node prefix length = %lu, prefix = %s update data object now\n", root->thisNodeID_, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
            root->data_ = newDataObj;
            return true;
        } else {
            if (root != nullptr) {
                debug_error("[ERROR] Could not reach target node (not leaf) ID = %lu, node prefix length = %lu, prefix = %s\n", root->thisNodeID_, root->currentNodePrefix.size(), root->currentNodePrefix.c_str());
            } else {
                debug_error("[ERROR] Could not reach target node (not exist) pointer = %p\n", (void*)root);
            }
            return false;
        }
    }
};

}
