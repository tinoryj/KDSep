#ifndef __KVSERVER_HH__
#define __KVSERVER_HH__

#include <atomic>
#include <vector>
#include "indexBasedStore/define.hh"
#include "indexBasedStore/ds/lru.hh"
#include "interface/deltaKVOptions.hpp"
#include "deviceManager.hh"
#include "keyManager.hh"
#include "valueManager.hh"
#include "segmentGroupManager.hh"
#include "logManager.hh"

/**
 * KvServer -- Interface for applications
 */

namespace DELTAKV_NAMESPACE {

class KvServer {
public:
    KvServer();
    KvServer(DeviceManager *deviceManager, rocksdb::DB* lsm);
    ~KvServer();

    bool putValue (const char *key, len_t keySize, const char *value, len_t valueSize, externalIndexInfo& storageInfoVec);
    bool getValue (const char *key, len_t keySize, char *&value, len_t &valueSize, externalIndexInfo storageInfoVec, bool timed = true);
//    void getRangeValues(char *startingKey, uint32_t numKeys, std::vector<char*> &keys, std::vector<char*> &values, std::vector<len_t> &valueSize);
//    bool delValue (char *key, len_t keySize);
    
    bool restoreVLog(std::map<std::string, externalIndexInfo>& keyValues);
    bool flushBuffer();
    size_t gc(bool all = false);

    void printStorageUsage(FILE *out = stdout);
    void printGroups(FILE *out = stdout);
    void printBufferUsage(FILE *out = stdout);
    void printKeyCacheUsage(FILE *out = stdout);
    void printKeyStats(FILE *out = stdout);
    void printValueSlaveStats(FILE *out = stdout);
    void printGCStats(FILE *out = stdout);

private:
    KeyManager *_keyManager;
    ValueManager *_valueManager;
    DeviceManager *_deviceManager;
    LogManager *_logManager;
    GCManager *_gcManager;
    SegmentGroupManager *_segmentGroupManager;

    boost::threadpool::pool _scanthreads;

    struct {
        LruList* lru;
    } _cache;

    bool _freeDeviceManager; 
    bool checkKeySize(len_t &keySize);

//    void getValueMt(char *key, len_t keySize, char *&value, len_t &valueSize, ValueLocation valueLoc, uint8_t &ret, std::atomic<size_t> &keysInProcess);
};

}
#endif
