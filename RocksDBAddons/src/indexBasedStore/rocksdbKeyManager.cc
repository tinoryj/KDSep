#include "indexBasedStore/rocksdbKeyManager.hh"
#include "indexBasedStore/statsRecorder.hh"
#include "indexBasedStore/util/debug.hh"

namespace DELTAKV_NAMESPACE {

RocksDBKeyManager::RocksDBKeyManager(rocksdb::DB* lsm) {
    _lsm = lsm; 
}

RocksDBKeyManager::~RocksDBKeyManager() {
//    if (_cache.lru != 0)
//        delete _cache.lru;
//    delete _lsm;
}
//
//bool RocksDBKeyManager::writeKey (char *keyStr, ValueLocation valueLoc, int needCache) {
//    bool ret = false;
//
//    // update cache
//    if (_cache.lru && needCache) {
//        unsigned char* key = (unsigned char*) keyStr;
//        if (needCache > 1) {
//            STAT_TIME_PROCESS(_cache.lru->update(key, valueLoc.segmentId), StatsType::KEY_UPDATE_CACHE);
//        } else {
//            STAT_TIME_PROCESS(_cache.lru->insert(key, valueLoc.segmentId), StatsType::KEY_SET_CACHE);
//        }
//    }
//
//    rocksdb::WriteOptions wopt;
//    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
//
//    // put the key into LSM-tree
//    STAT_TIME_PROCESS(ret = _lsm->Put(wopt, rocksdb::Slice(keyStr, KEY_SIZE), rocksdb::Slice(valueLoc.serialize())).ok(), KEY_SET_LSM);
//    return ret;
//}
//
//bool RocksDBKeyManager::writeKeyBatch (std::vector<char *> keys, std::vector<ValueLocation> valueLocs, int needCache) {
//    bool ret = true;
//    assert(keys.size() == valueLocs.size());
//    if (keys.empty())
//        return ret;
//
//    // update to LSM-tree
//    rocksdb::WriteBatch batch;
//    for (size_t i = 0; i < keys.size(); i++) {
//        // update cache if needed
//        if (_cache.lru && needCache) {
//            if (needCache > 1) {
//                STAT_TIME_PROCESS(_cache.lru->update((unsigned char*) keys.at(i), valueLocs.at(i).segmentId), StatsType::KEY_UPDATE_CACHE);
//            } else {
//                STAT_TIME_PROCESS(_cache.lru->insert((unsigned char*) keys.at(i), valueLocs.at(i).segmentId), StatsType::KEY_SET_CACHE);
//            }
//        }
//        // construct the batch for LSM-tree write
//        batch.Put(rocksdb::Slice(keys.at(i), KEY_SIZE), rocksdb::Slice(valueLocs.at(i).serialize()));
//
//    }
//
//    rocksdb::WriteOptions wopt;
//    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
//
//    // put the keys into LSM-tree
//    STAT_TIME_PROCESS(ret = _lsm->Write(wopt, &batch).ok(), StatsType::KEY_SET_LSM_BATCH);
//    return ret;
//}
//
//bool RocksDBKeyManager::writeKeyBatch (std::vector<std::string> &keys, std::vector<ValueLocation> valueLocs, int needCache) {
//    bool ret = true;
//    assert(keys.size() == valueLocs.size());
//    if (keys.empty())
//        return ret;
//
//    // update to LSM-tree
//    rocksdb::WriteBatch batch;
//    for (size_t i = 0; i < keys.size(); i++) {
//        // update cache if needed
//        if (_cache.lru && needCache) {
//            if (needCache > 1) {
//                STAT_TIME_PROCESS(_cache.lru->update((unsigned char*) keys.at(i).c_str(), valueLocs.at(i).segmentId), StatsType::KEY_UPDATE_CACHE);
//            } else {
//                STAT_TIME_PROCESS(_cache.lru->insert((unsigned char*) keys.at(i).c_str(), valueLocs.at(i).segmentId), StatsType::KEY_SET_CACHE);
//            }
//        }
//        // construct the batch for LSM-tree write
//        batch.Put(rocksdb::Slice(keys.at(i)), rocksdb::Slice(valueLocs.at(i).serialize()));
//
//    }
//
//    rocksdb::WriteOptions wopt;
//    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
//
//    // put the keys into LSM-tree
//    STAT_TIME_PROCESS(ret = _lsm->Write(wopt, &batch).ok(), StatsType::KEY_SET_LSM_BATCH);
//    return ret;
//}
//
//bool RocksDBKeyManager::writeMeta (const char *keyStr, int keySize, std::string metadata) {
//    rocksdb::WriteOptions wopt;
//    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
//    //printf("META %.*s %s\n", keySize, keyStr, metadata.c_str());
//
//    return _lsm->Put(wopt, rocksdb::Slice(keyStr, keySize), rocksdb::Slice(metadata)).ok();
//    return true;
//}
//
//std::string RocksDBKeyManager::getMeta (const char *keyStr, int keySize) {
//    std::string value;
//
//    _lsm->Get(rocksdb::ReadOptions(), rocksdb::Slice(keyStr, keySize), &value);
//
//    return value;
//}

bool RocksDBKeyManager::mergeKeyBatch (std::vector<char* > keys, std::vector<ValueLocation> valueLocs) {
    bool ret = true;
    assert(keys.size() == valueLocs.size());
    if (keys.empty())
        return ret;

    rocksdb::Status s;

    // update to LSM-tree
    rocksdb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();

    // put the keys into LSM-tree
    Slice kslice, vslice;
    std::string valueString;
    for (int i = 0; i < (int)keys.size(); i++) {
        kslice = rocksdb::Slice(keys.at(i), KEY_SIZE);
        valueString = valueLocs[i].serializeIndexUpdate();
        vslice = rocksdb::Slice(valueString);
        STAT_TIME_PROCESS(s = _lsm->Merge(wopt, kslice, vslice), StatsType::MERGE_INDEX_UPDATE);
        if (!s.ok()) {
            debug_error("%s\n", s.ToString().c_str());
            return false;
        }
    }
    return ret;
}

bool RocksDBKeyManager::mergeKeyBatch (std::vector<std::string> &keys, std::vector<ValueLocation> valueLocs) {
    bool ret = true;
    assert(keys.size() == valueLocs.size());
    if (keys.empty())
        return ret;

    rocksdb::Status s;

    // update to LSM-tree
    rocksdb::WriteOptions wopt;
    wopt.sync = ConfigManager::getInstance().syncAfterWrite();

    // put the keys into LSM-tree
    Slice kslice, vslice;
    std::string valueString;
    for (int i = 0; i < (int)keys.size(); i++) {
        kslice = rocksdb::Slice(keys.at(i)); 
        vslice = rocksdb::Slice(valueLocs.at(i).serializeIndexUpdate());
        STAT_TIME_PROCESS(s = _lsm->Merge(wopt, kslice, vslice), StatsType::MERGE_INDEX_UPDATE);
        if (!s.ok()) {
            debug_error("%s\n", s.ToString().c_str());
            return false;
        }
    }
    return ret;
}

ValueLocation RocksDBKeyManager::getKey (const char *keyStr, bool checkExist) {
    std::string value;
    ValueLocation valueLoc;
    valueLoc.segmentId = INVALID_SEGMENT;
    // only use cache for checking before SET
//    if (checkExist && _cache.lru /* cache enabled */) {
//        STAT_TIME_PROCESS(valueLoc.segmentId = _cache.lru->get((unsigned char*) keyStr), StatsType::KEY_GET_CACHE);
//    }
    // if not in cache, search in the LSM-tree
    if (valueLoc.segmentId == INVALID_SEGMENT) {
        rocksdb::Slice key (keyStr, KEY_SIZE);
        rocksdb::Status status;
        STAT_TIME_PROCESS(status = _lsm->Get(rocksdb::ReadOptions(), key, &value), StatsType::KEY_GET_LSM);
        // value location found
        if (status.ok()) {
            valueLoc.deserialize(value);
            debug_info("valueLoc length %lu offset %lu segmentId %lu\n", valueLoc.length, valueLoc.offset, valueLoc.segmentId);
        }
    }
    return valueLoc;
}

void RocksDBKeyManager::getKeys (char *startingKey, uint32_t n, std::vector<char*> &keys, std::vector<ValueLocation> &locs) {
    // use the iterator to find the range of keys
    rocksdb::Iterator *it = _lsm->NewIterator(rocksdb::ReadOptions());
    it->Seek(rocksdb::Slice(startingKey));
    ValueLocation loc;
    char *key = 0;
    for (uint32_t i = 0; i < n && it->Valid(); i++, it->Next()) {
        key = new char[KEY_SIZE];
        memcpy(key, it->key().ToString().c_str(), KEY_SIZE);
        //printf("FIND (%u of %u) [%0x][%0x][%0x][%0x]\n", i, n, key[0], key[1], key[2], key[3]);
        keys.push_back(key);
        loc.deserialize(it->value().ToString());
        locs.push_back(loc);
        debug_info("loc length %lu offset %lu segmentId %lu\n", loc.length, loc.offset, loc.segmentId);
    }
    delete it;
}

//RocksDBKeyManager::RocksDBKeyIterator *RocksDBKeyManager::getKeyIterator (char *startingKey) {
//    rocksdb::Iterator *it = _lsm->NewIterator(rocksdb::ReadOptions());
//    it->Seek(rocksdb::Slice(startingKey));
//
//    RocksDBKeyManager::RocksDBKeyIterator *kit = new RocksDBKeyManager::RocksDBKeyIterator(it);
//    return kit;
//}

//bool RocksDBKeyManager::deleteKey (char *keyStr) {
//    // remove the key from cache
////    if (_cache.lru) {
////        _cache.lru->removeItem((unsigned char*) keyStr);
////    }
//
//    rocksdb::WriteOptions wopt;
//    wopt.sync = ConfigManager::getInstance().syncAfterWrite();
//
//
//    // remove the key from LSM-tree
//    return _lsm->Delete(wopt, rocksdb::Slice(keyStr, KEY_SIZE)).ok();
//}


void RocksDBKeyManager::printCacheUsage(FILE *out) {
    fprintf(out, 
            "Cache Usage (KeyManager):\n"
    );
//    if (_cache.lru) {
//        fprintf(out, 
//                " LRU\n"
//        );
//        _cache.lru->print(out, true);
//    }
    fprintf(out, 
            " Shadow Hash Table\n"
    );
}

void RocksDBKeyManager::printStats(FILE *out) {
    std::string stats;
    _lsm->GetProperty("rocksdb.stats", &stats);
    fprintf(out,
            "LSM (KeyManager):\n"
            "%s\n"
            , stats.c_str()
    );
}

}
