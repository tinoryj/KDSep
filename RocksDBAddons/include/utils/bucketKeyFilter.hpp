#pragma once

#include "utils/murmurHash.hpp"
#include "indexBasedStore/ds/bitmap.hh"
#include <unordered_set>

using namespace std;

namespace DELTAKV_NAMESPACE {

class BucketKeyFilter {
    public:
    BucketKeyFilter();
    ~BucketKeyFilter();
    bool Insert(const string& key);
    bool Insert(const char* str, size_t len);
    bool MayExist(const string& key);
    bool Erase(const string& key);
    void Clear();

    // keys are used only when bm is disabled.
    unordered_set<string> keys;

    // erased_keys are used only when bm is enabled.
    unordered_set<string> erased_keys; 
    BitMap* bm = nullptr;

    const int KEYS_THRESHOLD = 10;
    const int BITMAP_SIZE = 65536; 
    const int REBUILD_THRESHOLD = 20; 
    
    static int hash1(const string& s, int arrSize); 
    static int hash2(const string& s, int arrSize); 
    static int hash3(const string& s, int arrSize); 
    private:
    bool SingleInsertToBitmap(const string& key);
};

}
