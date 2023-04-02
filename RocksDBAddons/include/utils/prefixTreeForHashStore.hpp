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
        init(initBitNumber, maxFileNumber);
    }

    PrefixTreeForHashStore()
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
//        root_ = new prefixTreeNode;
    }

    ~PrefixTreeForHashStore()
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        for (int i = 0; i < (1 << fixed_bit_num_); i++) {
            stack<prefixTreeNode*> stk;
            prefixTreeNode *p = roots_[i], *pre = nullptr;

            // almost a template for post order traversal ...
            while (p != nullptr || !stk.empty()) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->left_child; // go down one level
                }

                if (!stk.empty()) {
                    p = stk.top(); // its left children are deleted
                    stk.pop();
                    if (p->right_child == nullptr || pre == p->right_child) {
                        debug_trace("delete p %s\n", p->current_prefix.c_str());
                        delete p;
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->right_child;
                    }
                }
            }
        }

        for (long unsigned int i = 0; i < targetDeleteVec.size(); i++) {
            if (targetDeleteVec[i] != nullptr) {
                if (targetDeleteVec[i]->file_op_ptr != nullptr) {
                    delete targetDeleteVec[i]->file_op_ptr;
                }
                delete targetDeleteVec[i];
            }
        }

        delete[] roots_;
    }

    void init(uint64_t initBitNumber, uint64_t maxFileNumber)
    {
        init_bit_num_ = initBitNumber;
        fixed_bit_num_ = initBitNumber - 1;
        fixed_bit_mask_ = (1ull << fixed_bit_num_) - 1;
        max_file_num_ = maxFileNumber;
        initializeTree();
    }

    uint64_t getRemainFileNumber()
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        if (max_file_num_ < current_file_num_) {
            debug_error("[ERROR] too many files! %lu v.s. %lu\n", 
                    max_file_num_, current_file_num_); 
            exit(1);
        }
        return max_file_num_ - current_file_num_;
    }

    uint64_t insert(const uint64_t& prefix_u64, hashStoreFileMetaDataHandler*&
            newData)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);

        if (current_file_num_ >= max_file_num_) {
            debug_error("[ERROR] Could note insert new node, since there are "
                    "too many files, number = %lu, threshold = %lu\n",
                    current_file_num_, max_file_num_);
            printNodeMap();
            return 0;
        }
        uint64_t insertAtLevel = fixed_bit_num_;
        bool status = addPrefixTreeNode(roots_[prefix_u64 & fixed_bit_mask_],
                prefix_u64, newData, insertAtLevel);
        if (status == true) {
            current_file_num_++;
            debug_trace("Insert to new node success at level = %lu, for prefix"
                    " = %lx, current file number = %lu\n", insertAtLevel,
                    prefix_u64, current_file_num_);
            return insertAtLevel;
        } else {
            debug_error("[ERROR] Insert to new node fail at level = %lu, for "
                    "prefix = %lx\n", insertAtLevel, prefix_u64);
            printNodeMap();
            return 0;
        }
    }

    pair<uint64_t, uint64_t> insertPairOfNodes(const uint64_t& prefix1,
            hashStoreFileMetaDataHandler*& newData1, const uint64_t& prefix2,
            hashStoreFileMetaDataHandler*& newData2)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);

        if (current_file_num_ >= (max_file_num_ + 1)) {
            debug_error("[ERROR] Could note insert new node, since there are"
                    " too many files, number = %lu, threshold = %lu\n",
                    current_file_num_, max_file_num_);
            printNodeMap();
            return make_pair(0, 0);
        }
        uint64_t insertAtLevel1 = fixed_bit_num_;
        uint64_t insertAtLevel2 = fixed_bit_num_;
        bool status = addPrefixTreeNode(roots_[prefix1 & fixed_bit_mask_],
                prefix1, newData1, insertAtLevel1);
        if (status == true) {
            current_file_num_++;
            debug_trace("Insert to first new node success at level = %lu, for "
                    "prefix = %lx, current file number = %lu\n", insertAtLevel1,
                    prefix1, current_file_num_);
            // add another node
            status = addPrefixTreeNode(roots_[prefix2 & fixed_bit_mask_],
                    prefix2, newData2, insertAtLevel2);
            if (status == true) {
                current_file_num_++;
                debug_trace("Insert to second new node success at level = %lu, "
                        "for prefix = %lx, current file number = %lu\n",
                        insertAtLevel2, prefix2, current_file_num_);
                return make_pair(insertAtLevel1, insertAtLevel2);
            } else {
                debug_error("[ERROR] Insert to second new node fail at level = "
                        "%lu, for prefix = %lx\n", 
                        insertAtLevel1, prefix1);
                printNodeMap();
                return make_pair(insertAtLevel1, insertAtLevel2);
            }
        } else {
            debug_error("[ERROR] Insert to first new node fail at level = %lu, "
                    "for prefix = %lx\n", insertAtLevel1, prefix1);
            printNodeMap();
            return make_pair(insertAtLevel1, insertAtLevel2);
        }
    }

    uint64_t insertWithFixedBitNumber(const uint64_t& prefix_u64, uint64_t
            fixedBitNumber, hashStoreFileMetaDataHandler*& newData)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);

        debug_info("Current file number = %lu, threshold = %lu\n",
                current_file_num_, max_file_num_);
        if (current_file_num_ >= max_file_num_) {
            debug_error("[ERROR] Could note insert new node, since there are "
                    "too many files, number = %lu, threshold = %lu\n",
                    current_file_num_, max_file_num_);
            printNodeMap();
            return 0;
        }
        uint64_t insertAtLevel = fixed_bit_num_;
        bool status = addPrefixTreeNodeWithFixedBitNumber(
                roots_[prefix_u64 & fixed_bit_mask_], prefix_u64,
                fixedBitNumber, newData, insertAtLevel);
        if (status == true) {
            debug_trace("Insert to new node with fixed bit number =  %lu, "
                    "success at level =  %lu, for prefix = %lx\n",
                    fixedBitNumber, insertAtLevel, prefix_u64);
            current_file_num_++;
            return insertAtLevel;
        } else {
            debug_error("[ERROR] Insert to new node with fixed bit number = "
                    "%lu, fail at level =  %lu, for prefix = %lx\n",
                    fixedBitNumber, insertAtLevel, prefix_u64);
            printNodeMap();
            return 0;
        }
    }

    bool get(const uint64_t prefix_u64,
            hashStoreFileMetaDataHandler*& newData, 
            uint64_t prefix_len = 64)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

        if (fixed_bit_num_ > prefix_len) {
            newData = nullptr;
            return false;
        }

        uint64_t find_at_level_id = 0;
        bool status = findPrefixTreeNode(roots_[prefix_u64 & fixed_bit_mask_],
                prefix_u64, newData, find_at_level_id, prefix_len);
        return status;
    }

    bool find(const uint64_t& prefix_u64, uint64_t& find_at_level_id)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        hashStoreFileMetaDataHandler* newData;
        bool status = findPrefixTreeNode(roots_[prefix_u64 & fixed_bit_mask_],
                prefix_u64, newData, find_at_level_id);
        return status;
    }

    bool remove(const uint64_t& prefix_u64, const uint64_t prefix_len, 
            uint64_t& find_at_level_id)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        bool status = markPrefixTreeNodeAsNonLeafNode(
                roots_[prefix_u64 & fixed_bit_mask_], prefix_u64, 
                prefix_len, find_at_level_id);
        if (status == true) {
            current_file_num_--;
            return true;
        } else {
            return false;
        }
    }

    bool mergeNodesToNewLeafNode(const uint64_t& prefix_u64, const uint64_t
            prefix_len, uint64_t& find_at_level_id)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        bool status = markPrefixTreeNodeAsNewLeafNodeAndDeleteChildren(
                roots_[prefix_u64 & fixed_bit_mask_], prefix_u64, prefix_len,
                find_at_level_id);
        if (status == true) {
            current_file_num_--;
            return true;
        } else {
            return false;
        }
    }

    bool updateDataObjectForTargetLeafNode(const uint64_t& prefix_u64, 
            const uint64_t& prefix_len, uint64_t& find_at_level_id, 
            hashStoreFileMetaDataHandler* newDataObj)
    {
        std::scoped_lock<std::shared_mutex> w_lock(nodeOperationMtx_);
        bool status = updateLeafNodeDataObject(
                roots_[prefix_u64 & fixed_bit_mask_], 
                prefix_u64, prefix_len, find_at_level_id, newDataObj);
        if (status == true) {
            return true;
        } else {
            return false;
        }
    }

    bool getCurrentValidNodes(vector<pair<string, hashStoreFileMetaDataHandler*>>& validObjectList)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

        for (int i = 0; i <= fixed_bit_mask_; i++) {
            prefixTreeNode *p = roots_[i], *pre = nullptr;
            stack<prefixTreeNode*> stk;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->left_child;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->right_child == nullptr || pre == p->right_child) {
                        if (p->is_leaf == true) {
                            validObjectList.push_back(make_pair(p->current_prefix, p->data));
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->right_child;
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

    bool getCurrentValidNodes(vector<pair<uint64_t,
            hashStoreFileMetaDataHandler*>>& validObjectList)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

        for (int i = 0; i <= fixed_bit_mask_; i++) {
            prefixTreeNode *p = roots_[i], *pre = nullptr;
            stack<prefixTreeNode*> stk;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->left_child;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->right_child == nullptr || pre == p->right_child) {
                        if (p->is_leaf == true) {
                            uint64_t k = p->prefix_u64 & ((1ull << 56) - 1);
                            k |= (p->prefix_len << 56);
                            validObjectList.push_back(make_pair(k, p->data));
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->right_child;
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

    bool getCurrentValidNodesNoKey(vector<hashStoreFileMetaDataHandler*>&
            validObjectList)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

        for (int i = 0; i <= fixed_bit_mask_; i++) {
            prefixTreeNode *p = roots_[i], *pre = nullptr;
            stack<prefixTreeNode*> stk;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->left_child;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->right_child == nullptr || pre == p->right_child) {
                        if (p->is_leaf == true) {
                            validObjectList.push_back(p->data);
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->right_child;
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

    bool getPossibleValidNodes(vector<pair<string, hashStoreFileMetaDataHandler*>>& validObjectList)
    {
        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);
        // post order
        for (int i = 0; i <= fixed_bit_mask_; i++) {
            stack<prefixTreeNode*> stk;
            prefixTreeNode *p = roots_[i], *pre = nullptr;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->left_child;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->right_child == nullptr || pre == p->right_child) {
                        if (p->current_prefix.size() != 0) {
                            validObjectList.push_back(make_pair(p->current_prefix, p->data));
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->right_child;
                    }
                }
            }
        }

        return (validObjectList.size() != 0);
    }

    bool getInValidNodes(vector<pair<string, hashStoreFileMetaDataHandler*>>& invalidObjectList)
    {

        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

        for (int i = 0; i <= fixed_bit_mask_; i++) {
            stack<prefixTreeNode*> stk;
            prefixTreeNode *p = roots_[i], *pre = nullptr;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->left_child;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->right_child == nullptr || pre == p->right_child) {
                        if (p->current_prefix.size() != 0 && p->is_leaf == false) {
                            invalidObjectList.push_back(make_pair(p->current_prefix, p->data));
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->right_child;
                    }
                }
            }
        }

        return (invalidObjectList.size() != 0);
    }

    bool getInvalidNodesNoKey(vector<hashStoreFileMetaDataHandler*>& invalidObjectList)
    {

        std::shared_lock<std::shared_mutex> r_lock(nodeOperationMtx_);

        for (int i = 0; i <= fixed_bit_mask_; i++) {
            stack<prefixTreeNode*> stk;
            prefixTreeNode *p = roots_[i], *pre = nullptr;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->left_child;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->right_child == nullptr || pre == p->right_child) {
                        if (p->current_prefix.size() != 0 && p->is_leaf == false) {
                            invalidObjectList.push_back(p->data);
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->right_child;
                    }
                }
            }
        }

        return (invalidObjectList.size() != 0);
    }

    void printNodeMap()
    {
        for (int i = 0; i <= fixed_bit_mask_; i++) {
            stack<prefixTreeNode*> stk;
            prefixTreeNode *p = roots_[i], *pre = nullptr;
            while (!stk.empty() || p != nullptr) {
                while (p != nullptr) {
                    stk.push(p);
                    p = p->left_child;
                }

                if (!stk.empty()) {
                    p = stk.top();
                    stk.pop();
                    if (p->right_child == nullptr || pre == p->right_child) {
                        if (p->current_prefix.size() != 0) {
                            debug_trace("Find node, is leaf node flag = %d, prefix length = %lu, linked prefix = %s\n", p->is_leaf, p->current_prefix.size(), p->current_prefix.c_str());
                        }
                        pre = p;
                        p = nullptr;
                    } else {
                        stk.push(p);
                        p = p->right_child;
                    }
                }
            }
        }
    }

    bool isMergableLength(uint64_t prefix_len) {
        return prefix_len > fixed_bit_num_;
    }

private:
    typedef struct prefixTreeNode {
        uint64_t node_id = 0;
        prefixTreeNode* left_child = nullptr; // 0
        prefixTreeNode* right_child = nullptr; // 1
        bool is_leaf = false;
        string current_prefix;
        uint64_t prefix_u64;
        uint64_t prefix_len;
        hashStoreFileMetaDataHandler* data = nullptr; //
    } prefixTreeNode;
    vector<hashStoreFileMetaDataHandler*> targetDeleteVec;
    std::shared_mutex nodeOperationMtx_;
    uint64_t nextNodeID_ = 0;
    uint64_t init_bit_num_ = 0;
    uint64_t fixed_bit_num_ = 0;
    uint64_t fixed_bit_mask_ = 0;
    uint64_t max_file_num_ = 0;
    uint64_t current_file_num_ = 0;
//    prefixTreeNode* root_;
    prefixTreeNode** roots_ = nullptr;

    // The previous fixed_bit_num_ layers are compacted to an array
    void initializeTree()
    {
        roots_ = new prefixTreeNode*[1 << fixed_bit_num_];
        for (int i = 0; i <= fixed_bit_mask_; i++) {
            roots_[i] = new prefixTreeNode;
            createPrefixTree(roots_[i], fixed_bit_num_);
        }
    }

    void createPrefixTree(prefixTreeNode* root, int lvl) {
        lvl++;
        root->is_leaf = false;
        root->node_id = nextNodeID_;
        nextNodeID_++;
        if (lvl != init_bit_num_) {
            root->left_child = new prefixTreeNode;
            root->right_child = new prefixTreeNode;
            createPrefixTree(root->left_child, lvl);
            createPrefixTree(root->right_child, lvl);
        } else {
            return;
        }
    }

    bool addPrefixTreeNode(prefixTreeNode* root, const uint64_t& prefix_u64,
            hashStoreFileMetaDataHandler* newDataObj, 
            uint64_t& insertAtLevelID)
    {
        uint64_t lvl = fixed_bit_num_;
        char prefixStr[64]; 
        for (int i = 0; i < lvl; i++) {
            prefixStr[i] = ((prefix_u64 & (1 << i)) > 0) + '0';
        }
        for (; lvl < 64; lvl++) {
            // cout << "Current level = " << lvl << endl;
            if ((prefix_u64 & (1 << lvl)) == 0) {
                prefixStr[lvl] = '0';
                // go to left if 0
                if (root->left_child == nullptr) {
                    root->left_child = new prefixTreeNode;
                    // insert at next level
                    root = root->left_child;
                    root->is_leaf = true;
                    root->data = newDataObj;
                    root->current_prefix = string(prefixStr, lvl + 1);
                    root->prefix_u64 = prefix_u64; 
                    root->prefix_len = lvl + 1;
                    root->node_id = nextNodeID_;
                    nextNodeID_++;
                    insertAtLevelID = lvl + 1;
                    return true;
                } else {
                    root = root->left_child;
                    if (root->is_leaf == true) {
                        root->is_leaf = false;
                        current_file_num_--;
                        debug_info("Meet old leaf node (left) during add,"
                                " should mark as not leaf node, current level ="
                                " %lu, node prefix length = %lu, prefix = %s,"
                                " currentFilnumber = %lu\n", lvl,
                                root->current_prefix.size(),
                                root->current_prefix.c_str(),
                                current_file_num_);
                        break;
                    } else {
                        continue;
                    }
                }
            } else {
                prefixStr[lvl] = '1';
                // go to right if 1
                if (root->right_child == nullptr) {
                    root->right_child = new prefixTreeNode;
                    // insert at next level
                    root = root->right_child;
                    root->is_leaf = true;
                    root->data = newDataObj;
                    root->current_prefix = string(prefixStr, lvl + 1);
                    root->prefix_u64 = prefix_u64;
                    root->prefix_len = lvl + 1;
                    root->node_id = nextNodeID_;
                    nextNodeID_++;
                    insertAtLevelID = lvl + 1;
                    return true;
                } else {
                    root = root->right_child;
                    if (root->is_leaf == true) {
                        root->is_leaf = false;
                        current_file_num_--;
                        debug_info("Meet old leaf node (right) during add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s, currentFilnumber = %lu\n", lvl, root->current_prefix.size(), root->current_prefix.c_str(), current_file_num_);
                        break;
                    } else {
                        continue;
                    }
                }
            }
        }
        lvl++;
        if ((prefix_u64 & (1 << lvl)) == 0) {
            prefixStr[lvl] = '0';
            // go to left if 0
            if (root->left_child == nullptr) {
                root->left_child = new prefixTreeNode;
                // insert at next level
                root = root->left_child;
                root->is_leaf = true;
                root->data = newDataObj;
                root->current_prefix = string(prefixStr, lvl + 1);
                root->prefix_u64 = prefix_u64;
                root->prefix_len = lvl + 1;
                root->node_id = nextNodeID_;
                nextNodeID_++;
                insertAtLevelID = lvl + 1;
                return true;
            } else {
                debug_error("[ERROR] Find left node after leaf node mark, error, current level = %lu, node prefix length = %lu, prefix = %s\n", lvl, root->current_prefix.size(), root->current_prefix.c_str());
                return false;
            }
        } else {
            prefixStr[lvl] = '1';
            // go to right if 1
            if (root->right_child == nullptr) {
                root->right_child = new prefixTreeNode;
                // insert at next level
                root = root->right_child;
                root->is_leaf = true;
                root->data = newDataObj;
                root->current_prefix = string(prefixStr, lvl + 1);
                root->prefix_u64 = prefix_u64;
                root->prefix_len = lvl + 1;
                root->node_id = nextNodeID_;
                nextNodeID_++;
                insertAtLevelID = lvl + 1;
                return true;
            } else {
                debug_error("[ERROR] Find right node after leaf node mark, error, current level = %lu, node prefix length = %lu, prefix = %s\n", lvl, root->current_prefix.size(), root->current_prefix.c_str());
                return false;
            }
        }
        return false;
    }

    bool addPrefixTreeNodeWithFixedBitNumber(prefixTreeNode* root, 
            const uint64_t& prefix_u64, uint64_t fixedBitNumber,
            hashStoreFileMetaDataHandler* newDataObj, 
            uint64_t& insertAtLevelID)
    {
        uint64_t lvl = fixed_bit_num_;
        char prefixStr[64]; 
        for (int i = 0; i < lvl; i++) {
            prefixStr[i] = ((prefix_u64 & (1 << i)) > 0) + '0';
        }
        for (; lvl < fixedBitNumber - 1; lvl++) {
            // cout << "Current level = " << lvl << endl;
            if ((prefix_u64 & (1 << lvl)) == 0) {
                prefixStr[lvl] = '0';
                // go to left if 0
                if (root->left_child == nullptr) {
                    root->left_child = new prefixTreeNode;
                    root = root->left_child;
                    root->is_leaf = false;
                    root->node_id = nextNodeID_;
                    nextNodeID_++;
                } else {
                    root = root->left_child;
                    if (root->is_leaf == true) {
                        root->is_leaf = false;
                        debug_info("Meet old leaf node (left) during fixed bit number add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s\n", lvl, root->current_prefix.size(), root->current_prefix.c_str());
                        continue;
                    } else {
                        continue;
                    }
                }
            } else {
                prefixStr[lvl] = '1';
                // go to right if 1
                if (root->right_child == nullptr) {
                    root->right_child = new prefixTreeNode;
                    root = root->right_child;
                    root->is_leaf = false;
                    root->node_id = nextNodeID_;
                    nextNodeID_++;
                } else {
                    root = root->right_child;
                    if (root->is_leaf == true) {
                        root->is_leaf = false;
                        debug_info("Meet old leaf node (right) during fixed bit number add, should mark as not leaf node, current level = %lu, node prefix length = %lu, prefix = %s\n", lvl, root->current_prefix.size(), root->current_prefix.c_str());
                        continue;
                    } else {
                        continue;
                    }
                }
            }
        }
        lvl++;
        if ((prefix_u64 & (1 << (fixedBitNumber - 1))) == 0) {
            prefixStr[fixedBitNumber - 1] = '0';
            // go to left if 0
            if (root->left_child == nullptr) {
                root->left_child = new prefixTreeNode;
                root = root->left_child;
                root->is_leaf = true;
                root->data = newDataObj;
                root->current_prefix = string(prefixStr, lvl);
                root->prefix_u64 = prefix_u64;
                root->prefix_len = lvl + 1;
                root->node_id = nextNodeID_;
                nextNodeID_++;
                insertAtLevelID = lvl;
                return true;
            } else {
                debug_error("[ERROR] Find left node after leaf node mark, error during fixed bit number add, could not add new node, current level = %lu, node prefix length = %lu, prefix = %s\n", lvl, root->current_prefix.size(), root->current_prefix.c_str());
                return false;
            }
        } else {
            prefixStr[fixedBitNumber - 1] = '1';
            // go to right if 1
            if (root->right_child == nullptr) {
                root->right_child = new prefixTreeNode;
                root = root->right_child;
                root->is_leaf = true;
                root->data = newDataObj;
                root->current_prefix = string(prefixStr, lvl);
                root->prefix_u64 = prefix_u64;
                root->prefix_len = lvl + 1;
                root->node_id = nextNodeID_;
                nextNodeID_++;
                insertAtLevelID = lvl;
                return true;
            } else {
                debug_error("[ERROR] Find right node after leaf node mark, error during fixed bit number add, could not add new node, current level = %lu, node prefix length = %lu, prefix = %s\n", lvl, root->current_prefix.size(), root->current_prefix.c_str());
                return false;
            }
        }
        return false;
    }

    bool findPrefixTreeNode(prefixTreeNode* root, const uint64_t& prefix_u64,
            hashStoreFileMetaDataHandler*& currentDataTObj, 
            uint64_t& find_at_level_id, uint64_t prefix_len = 64)
    {
        uint64_t lvl = fixed_bit_num_;
        for (; lvl < prefix_len; lvl++) {
            if ((prefix_u64 & (1 << lvl)) == 0) {
                // go to left if 0
                if (root->is_leaf == true) {
                    currentDataTObj = root->data;
                    find_at_level_id = lvl;
                    return true;
                } else {
                    if (root->left_child == nullptr) {
                        debug_info("No left node, but this node is not leaf"
                                " node, not exist. current level = %lu, node"
                                " prefix length = %lu, prefix = %s\n", lvl,
                                root->current_prefix.size(),
                                root->current_prefix.c_str());
                        return false;
                    } else {
                        root = root->left_child;
                    }
                }
            } else {
                // go to right if 1
                if (root->is_leaf == true) {
                    currentDataTObj = root->data;
                    find_at_level_id = lvl;
                    return true;
                } else {
                    if (root->right_child == nullptr) {
                        debug_info("No right node, but this node is not leaf"
                                " node, not exist. current level = %lu, node"
                                " prefix length = %lu, prefix = %s\n", lvl,
                                root->current_prefix.size(),
                                root->current_prefix.c_str());
                        return false;
                    } else {
                        root = root->right_child;
                    }
                }
            }
        }
        if (root == nullptr) {
            debug_info("This node not exist, may be deleted. current level = %lu, node prefix length = %lu, prefix = %s\n", lvl, root->current_prefix.size(), root->current_prefix.c_str());
            return false;
        } else {
            if (root->is_leaf == true) {
                currentDataTObj = root->data;
                find_at_level_id = lvl;
                return true;
            } else {
                debug_info("This node is not leaf node. current level = %lu, node prefix length = %lu, prefix = %s\n", lvl, root->current_prefix.size(), root->current_prefix.c_str());
                return false;
            }
        }
    }

    bool markPrefixTreeNodeAsNonLeafNode(prefixTreeNode* root, 
            const uint64_t& prefix_u64, const uint64_t prefix_len, 
            uint64_t& find_at_level_id)
    {
        find_at_level_id = fixed_bit_num_;
        for (uint64_t lvl = fixed_bit_num_; lvl < prefix_len; lvl++) {
            if ((prefix_u64 & (1 << lvl)) == 0) {
                // go to left if 0
                root = root->left_child;
            } else {
                // go to right if 1
                root = root->right_child;
            }
            find_at_level_id++;
        }
        if (root != nullptr && root->is_leaf == true) {
            debug_trace("Find leaf node ID = %lu, node prefix length = %lu, prefix = %s remove it now\n", root->node_id, root->current_prefix.size(), root->current_prefix.c_str());
            root->is_leaf = false;
            return true;
        } else {
            if (root != nullptr) {
                debug_error("[ERROR] Could not delete target node (not leaf) ID = %lu, node prefix length = %lu, prefix = %s remove it now\n", root->node_id, root->current_prefix.size(), root->current_prefix.c_str());
            } else {
                debug_error("[ERROR] Could not delete target node (not exist) pointer = %p\n", (void*)root);
            }
            return false;
        }
    }

    /* for merging two leaf nodes */
    bool markPrefixTreeNodeAsNewLeafNodeAndDeleteChildren(prefixTreeNode* root,
            const uint64_t& prefix_u64, const uint64_t prefix_len, 
            uint64_t& find_at_level_id)
    {
        auto& pre_root = root;
        find_at_level_id = fixed_bit_num_;
        char prefix_str[64];
        for (int i = 0; i < find_at_level_id; i++) {
            prefix_str[i] = ((prefix_u64 & (1 << i)) > 0) + '0';
        }
        for (uint64_t lvl = fixed_bit_num_; lvl < prefix_len; lvl++) {
            if ((prefix_u64 & (1 << lvl)) == 0) {
                // go to left if 0
                prefix_str[lvl] = '0';
                root = root->left_child;
            } else {
                // go to right if 1
                prefix_str[lvl] = '1';
                root = root->right_child;
            }
            find_at_level_id++;
        }
        if (root != nullptr && root->is_leaf == false) {
            debug_trace("Find non leaf node ID = %lu, node prefix length = "
                    "%lu, prefix = %s mark it as leaf now\n", root->node_id,
                    root->current_prefix.size(), root->current_prefix.c_str());
            root->is_leaf = true;
            root->current_prefix = string(prefix_str, prefix_len);
            root->prefix_u64 = prefix_u64;
            root->prefix_len = prefix_len;

            if (root->left_child->data != nullptr) {
                targetDeleteVec.push_back(root->left_child->data);
            }
            if (root->right_child->data != nullptr) {
                targetDeleteVec.push_back(root->right_child->data);
            }
            delete root->left_child;
            delete root->right_child;
            root->left_child = nullptr;
            root->right_child = nullptr;
            return true;
        } else {
            if (root != nullptr) {
                debug_error("[ERROR] Could not delete target node (not leaf)" 
                        " ID = %lu, node prefix length = %lu, prefix = %s "
                        " remove it now. pre_root %p root %p\n", 
                        root->node_id, prefix_len,
                        root->current_prefix.c_str(),
                        pre_root, root);
            } else {
                debug_error("[ERROR] Could not delete target node (not exist) pointer = %p\n", (void*)root);
            }
            return false;
        }
    }

    bool updateLeafNodeDataObject(prefixTreeNode* root, const uint64_t&
            prefix_u64, const uint64_t prefix_len, uint64_t& find_at_level_id,
            hashStoreFileMetaDataHandler* newDataObj)
    {
        uint64_t searchLevelNumber = prefix_len;
        find_at_level_id = fixed_bit_num_;
        for (uint64_t lvl = fixed_bit_num_; lvl < searchLevelNumber; lvl++) {
            if ((prefix_u64 & (1 << lvl)) == 0) {
                // go to left if 0
                root = root->left_child;
            } else {
                // go to right if 1
                root = root->right_child;
            }
            find_at_level_id++;
        }
        if (root != nullptr && root->is_leaf == true) {
            debug_trace("Find target leaf node ID = %lu, node prefix length = %lu, prefix = %s update data object now\n", root->node_id, root->current_prefix.size(), root->current_prefix.c_str());
            root->data = newDataObj;
            return true;
        } else {
            if (root != nullptr) {
                debug_error("[ERROR] Could not reach target node (not leaf) ID = %lu, node prefix length = %lu, prefix = %s\n", root->node_id, root->current_prefix.size(), root->current_prefix.c_str());
            } else {
                debug_error("[ERROR] Could not reach target node (not exist) pointer = %p\n", (void*)root);
            }
            return false;
        }
    }
};

}
