#include "utils/bucketKeyFilter.hpp"

namespace DELTAKV_NAMESPACE {

int BucketKeyFilter::hash1(const string& s, int arrSize) {
    int hash = 0;
    for (auto i = 0; i < s.size(); i++) {
        hash = hash + ((int)s[i]);
        hash = hash % arrSize;
    }
    return hash;
}

int BucketKeyFilter::hash2(const string& s, int arrSize) {
    int hash = 1;
    for (auto i = 0; i < s.size(); i++) {
        hash = hash + pow(19, i) * s[i];
        hash = hash % arrSize;
    }
    return hash % arrSize;
}

int BucketKeyFilter::hash3(const string& s, int arrSize) {
    int hash = 7;
    for (auto i = 0; i < s.size(); i++) {
        hash = (hash * 31 + s[i]) % arrSize; 
    }
    return hash;
}

BucketKeyFilter::BucketKeyFilter() {
}

BucketKeyFilter::~BucketKeyFilter() {
    if (bm != nullptr) {
        delete bm;
        bm = nullptr;
    }
}

bool BucketKeyFilter::SingleInsertToBitmap(const string& key) {
    if (bm == nullptr) bm = new BitMap(BITMAP_SIZE);
    bm->setBit(hash1(key, BITMAP_SIZE));
    bm->setBit(hash2(key, BITMAP_SIZE));
    bm->setBit(hash3(key, BITMAP_SIZE));
    return true;
}

bool BucketKeyFilter::Insert(const string& key) {
    if (bm == nullptr) {
        keys.insert(key);
        if (keys.size() >= KEYS_THRESHOLD) {
            for (auto& it : keys) {
                SingleInsertToBitmap(it);
            }
            keys.clear();
        }
    } else {
//        if (keys.size() >= KEYS_THRESHOLD) {
//            keys.clear();
//        }
//        keys.insert(key);
        if (erased_keys.count(key)) {
            erased_keys.erase(key);
        }
        SingleInsertToBitmap(key);
    }
    return true;
}

bool BucketKeyFilter::Insert(const char* str, size_t len) {
    return Insert(string(str, len));
}

bool BucketKeyFilter::MayExist(const string& key) {
    if (bm == nullptr) {
        return keys.count(key);
    } else {
        // check the erased_keys!
        if (keys.count(key)) {
            return true;
        }
        if (erased_keys.count(key)) {
            return false;
        }
        if (bm->getBit(hash1(key, BITMAP_SIZE)) == false) {
            return false;
        }
        if (bm->getBit(hash2(key, BITMAP_SIZE)) == false) {
            return false;
        }
        if (bm->getBit(hash3(key, BITMAP_SIZE)) == false) {
            return false;
        }
        return true;
    }
}

//bool BucketKeyFilter::Exist(const string& key) {
//    return keys.count(key);
//}

bool BucketKeyFilter::Erase(const string& key) {
    if (bm == nullptr) {
        auto setIt = keys.find(key);
        if (setIt == keys.end()) {
            keys.erase(setIt);
        }
    } else {
        erased_keys.insert(key);
//        if (keys.count(key)) {
//            keys.erase(key);
//        }
//        if (erased_keys.size() > REBUILD_THRESHOLD) {
//            debug_error("[WARN] erased_keys too many: %d v.s. threshold %d\n",
//                    (int)erased_keys.size(), REBUILD_THRESHOLD);
//        }
    }
    return true;
}

void BucketKeyFilter::Clear() {
    delete bm;
    bm = nullptr;
    keys.clear();
    erased_keys.clear();
}

}
