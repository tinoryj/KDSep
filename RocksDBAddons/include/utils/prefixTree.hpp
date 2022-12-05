#pragma once

#include <bits/stdc++.h>
#include <shared_mutex>
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
            std::unique_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
            createPrefixTree(rootNode_, 0);
        }
    }

    PrefixTree()
    {
    }

    ~PrefixTree()
    {
        for (auto it : nodeMap_) {
            cerr << "Cleanup node ID = " << it.first << ", is leaf node flag = " << it.second->isLeafNodeFlag_ << ", linked prefix = " << it.second->currentNodePrefix << endl;
            delete it.second;
        }
    }

    void init(uint64_t initBitNumber, uint64_t maxBitNumber)
    {
        initBitNumber_ = initBitNumber;
        maxBitNumber_ = maxBitNumber;
        rootNode_ = new prefixTreeNode;
        {
            std::unique_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
            createPrefixTree(rootNode_, 0);
        }
    }

    uint64_t insert(string prefixStr, dataT& newData)
    {
        uint64_t insertAtLevel = 0;
        bool status;
        {
            std::unique_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
            status = addPrefixTreeNode(rootNode_, prefixStr, newData, insertAtLevel);
        }
        if (status == true) {
            cerr << "Insert to new node success at level = " << insertAtLevel << ", for prefix = " << prefixStr << ", Current Node map size = " << nodeMap_.size() << endl;
            return insertAtLevel;
        } else {
            cerr << "Insert to new node fail at level = " << insertAtLevel << ", for prefix = " << prefixStr << ", Current Node map size = " << nodeMap_.size() << endl;
            printNodeMap();
            return 0;
        }
    }

    uint64_t insertWithFixedBitNumber(string prefixStr, uint64_t fixedBitNumber, dataT& newData)
    {
        uint64_t insertAtLevel = 0;
        bool status;
        {
            std::unique_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
            status = addPrefixTreeNodeWithFixedBitNumber(rootNode_, prefixStr, fixedBitNumber, newData, insertAtLevel);
        }
        if (status == true) {
            cerr << "Insert to new node with fixed bit number = " << fixedBitNumber << " success at level = " << insertAtLevel << ", for prefix = " << prefixStr << ", Current Node map size = " << nodeMap_.size() << endl;
            return insertAtLevel;
        } else {
            cerr << "Insert to new node with fixed bit number = " << fixedBitNumber << " fail at level = " << insertAtLevel << ", for prefix = " << prefixStr << ", Current Node map size = " << nodeMap_.size() << endl;
            printNodeMap();
            return 0;
        }
    }

    bool get(string prefixStr, dataT& newData)
    {
        uint64_t findAtLevelID = 0;
        bool status;
        {
            std::unique_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
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
            std::unique_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
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
            std::unique_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
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
            std::unique_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
            for (auto it : nodeMap_) {
                if (it.second->isLeafNodeFlag_ == true) {
                    validObjectList.push_back(make_pair(it.second->currentNodePrefix, it.second->data_));
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
            std::unique_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
            for (auto it : nodeMap_) {
                if (it.second->currentNodePrefix.size() != 0) {
                    validObjectList.push_back(make_pair(it.second->currentNodePrefix, it.second->data_));
                }
            }
        }
        if (validObjectList.size() != 0) {
            return true;
        } else {
            return false;
        }
    }

    void printNodeMap()
    {
        std::unique_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        for (auto it : nodeMap_) {
            if (it.second->currentNodePrefix.size() != 0) {
                cerr << "Find node ID = " << it.first << ", is leaf node flag = " << it.second->isLeafNodeFlag_ << ", linked prefix = " << it.second->currentNodePrefix << endl;
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
    unordered_map<uint64_t, prefixTreeNode*> nodeMap_;
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
        nodeMap_.insert(make_pair(root->thisNodeID_, root));
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
        for (; currentLevel < maxBitNumber_; currentLevel++) {
            // cout << "Current level = " << currentLevel << endl;
            if (bitBasedPrefixStr.at(currentLevel) == '0') {
                // go to left if 0
                if (root->leftChildNodePtr_ == nullptr) {
                    root->leftChildNodePtr_ = new prefixTreeNode;
                    // cerr << "Create new left node at level = " << currentLevel + 1 << endl;
                    root = root->leftChildNodePtr_;
                    root->isLeafNodeFlag_ = true;
                    root->data_ = newDataObj;
                    root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel + 1);
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                    nodeMap_.insert(make_pair(root->thisNodeID_, root));
                    insertAtLevelID = currentLevel + 1;
                    // cerr << "Current node ID = " << root->thisNodeID_ << endl;
                    return true;
                } else {
                    root = root->leftChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        cerr << "Meet old leaf node (left) during add, should mark as not leaf node" << endl;
                        break;
                    } else {
                        continue;
                    }
                }
            } else {
                // go to right if 1
                if (root->rightChildNodePtr_ == nullptr) {
                    root->rightChildNodePtr_ = new prefixTreeNode;
                    // cerr << "Create new right node at level = " << currentLevel + 1 << endl;
                    root = root->rightChildNodePtr_;
                    root->isLeafNodeFlag_ = true;
                    root->data_ = newDataObj;
                    root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel + 1);
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                    nodeMap_.insert(make_pair(root->thisNodeID_, root));
                    insertAtLevelID = currentLevel + 1;
                    // cerr << "Current node ID = " << root->thisNodeID_ << endl;
                    return true;
                } else {
                    root = root->rightChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        cerr << "Meet old leaf node (right) during add, should mark as not leaf node" << endl;
                        break;
                    } else {
                        continue;
                    }
                }
            }
        }
        currentLevel++;
        if (currentLevel >= maxBitNumber_) {
            cerr << "Reached max bit number, could not add new node" << endl;
            return false;
        }
        if (bitBasedPrefixStr.at(currentLevel) == '0') {
            // go to left if 0
            if (root->leftChildNodePtr_ == nullptr) {
                root->leftChildNodePtr_ = new prefixTreeNode;
                // cerr << "Create new left node at level = " << currentLevel + 1 << endl;
                root = root->leftChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel + 1);
                root->thisNodeID_ = nextNodeID_;
                nextNodeID_++;
                nodeMap_.insert(make_pair(root->thisNodeID_, root));
                insertAtLevelID = currentLevel + 1;
                // cerr << "Current node ID = " << root->thisNodeID_ << endl;
                return true;
            } else {
                cerr << "find left node after leaf node mark, error" << endl;
                return false;
            }
        } else {
            // go to right if 1
            if (root->rightChildNodePtr_ == nullptr) {
                root->rightChildNodePtr_ = new prefixTreeNode;
                // cerr << "Create new right node at level = " << currentLevel + 1 << endl;
                root = root->rightChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel + 1);
                root->thisNodeID_ = nextNodeID_;
                nextNodeID_++;
                nodeMap_.insert(make_pair(root->thisNodeID_, root));
                insertAtLevelID = currentLevel + 1;
                // cerr << "Current node ID = " << root->thisNodeID_ << endl;
                return true;
            } else {
                cerr << "find right node after leaf node mark, error" << endl;
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
                    // cerr << "Create new left node at level = " << currentLevel + 1 << endl;
                    root = root->leftChildNodePtr_;
                    root->isLeafNodeFlag_ = false;
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                    nodeMap_.insert(make_pair(root->thisNodeID_, root));
                } else {
                    root = root->leftChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        cerr << "[Fixed bit number insert] Meet old leaf node (left) during fixed bit number add, should mark as not leaf node" << endl;
                        continue;
                    } else {
                        continue;
                    }
                }
            } else {
                // go to right if 1
                if (root->rightChildNodePtr_ == nullptr) {
                    root->rightChildNodePtr_ = new prefixTreeNode;
                    // cerr << "Create new left node at level = " << currentLevel + 1 << endl;
                    root = root->rightChildNodePtr_;
                    root->isLeafNodeFlag_ = false;
                    root->thisNodeID_ = nextNodeID_;
                    nextNodeID_++;
                    nodeMap_.insert(make_pair(root->thisNodeID_, root));
                } else {
                    root = root->rightChildNodePtr_;
                    if (root->isLeafNodeFlag_ == true) {
                        root->isLeafNodeFlag_ = false;
                        cerr << "[Fixed bit number insert] Meet old leaf node (right) during fixed bit number add, should mark as not leaf node" << endl;
                        continue;
                    } else {
                        continue;
                    }
                }
            }
        }
        currentLevel++;
        if (currentLevel >= maxBitNumber_) {
            cerr << "[Fixed bit number insert] Reached max bit number, could not add new node" << endl;
            return false;
        }
        if (bitBasedPrefixStr.at(fixedBitNumber - 1) == '0') {
            // go to left if 0
            if (root->leftChildNodePtr_ == nullptr) {
                root->leftChildNodePtr_ = new prefixTreeNode;
                // cerr << "Create new left node at level = " << currentLevel + 1 << endl;
                root = root->leftChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel);
                root->thisNodeID_ = nextNodeID_;
                nextNodeID_++;
                nodeMap_.insert(make_pair(root->thisNodeID_, root));
                insertAtLevelID = currentLevel;
                // cerr << "Current node ID = " << root->thisNodeID_ << endl;
                return true;
            } else {
                cerr << "[Fixed bit number insert] find left node after leaf node mark, error" << endl;
                return false;
            }
        } else {
            // go to right if 1
            if (root->rightChildNodePtr_ == nullptr) {
                root->rightChildNodePtr_ = new prefixTreeNode;
                // cerr << "Create new right node at level = " << currentLevel + 1 << endl;
                root = root->rightChildNodePtr_;
                root->isLeafNodeFlag_ = true;
                root->data_ = newDataObj;
                root->currentNodePrefix = bitBasedPrefixStr.substr(0, currentLevel);
                root->thisNodeID_ = nextNodeID_;
                nextNodeID_++;
                nodeMap_.insert(make_pair(root->thisNodeID_, root));
                insertAtLevelID = currentLevel;
                // cerr << "Current node ID = " << root->thisNodeID_ << endl;
                return true;
            } else {
                cerr << "[Fixed bit number insert] find right node after leaf node mark, error" << endl;
                return false;
            }
        }
        return false;
    }

    bool findPrefixTreeNode(prefixTreeNode* root, string bitBasedPrefixStr, dataT& currentDataTObj, uint64_t& findAtLevelID)
    {
        uint64_t currentLevel = 0;
        for (; currentLevel < maxBitNumber_; currentLevel++) {
            // cerr << "Current level = " << currentLevel << ", Current node ID = " << root->thisNodeID_ << endl;
            if (bitBasedPrefixStr.at(currentLevel) == '0') {
                // go to left if 0
                if (root->leftChildNodePtr_ == nullptr) {
                    if (root->isLeafNodeFlag_ == true) {
                        // cerr << "Serach to leaf node, get data at level = " << currentLevel << endl;
                        currentDataTObj = root->data_;
                        findAtLevelID = currentLevel;
                        return true;
                    } else {
                        cerr << "No left node, but this node is not leaf node, not exist" << endl;
                        return false;
                    }
                } else {
                    // cerr << "Serach to new level = " << currentLevel + 1 << endl;
                    root = root->leftChildNodePtr_;
                }
            } else {
                // go to right if 1
                if (root->rightChildNodePtr_ == nullptr) {
                    if (root->isLeafNodeFlag_ == true) {
                        // cerr << "Serach to leaf node, get data at level = " << currentLevel << endl;
                        currentDataTObj = root->data_;
                        findAtLevelID = currentLevel;
                        return true;
                    } else {
                        cerr << "No right node, but this node is not leaf node, not exist" << endl;
                        return false;
                    }
                } else {
                    // cerr << "Serach to new level = " << currentLevel + 1 << endl;
                    root = root->rightChildNodePtr_;
                }
            }
        }
        if (root != nullptr && root->isLeafNodeFlag_ == true) {
            // cerr << "Serach to leaf node, get data at level = " << currentLevel << endl;
            currentDataTObj = root->data_;
            findAtLevelID = currentLevel;
            return true;
        } else {
            cerr << "This node may be deleted" << endl;
            return false;
        }
    }

    bool removePrefixTreeNode(prefixTreeNode* root, string bitBasedPrefixStr, uint64_t& findAtLevelID)
    {
        for (uint64_t currentLevel = 0; currentLevel < findAtLevelID; currentLevel++) {
            // cerr << "Current level = " << currentLevel << ", Current node ID = " << root->thisNodeID_ << endl;
            if (bitBasedPrefixStr.at(currentLevel) == '0') {
                // go to left if 0
                root = root->leftChildNodePtr_;
            } else {
                // go to right if 1
                root = root->rightChildNodePtr_;
            }
        }
        if (root != nullptr && root->isLeafNodeFlag_ == true) {
            cerr << "Find leaf node ID = " << root->thisNodeID_ << ", remove it now" << endl;
            root->isLeafNodeFlag_ = false;
            return true;
        } else {
            cerr << "Error, could not delete target node" << endl;
            return false;
        }
    }
};

}