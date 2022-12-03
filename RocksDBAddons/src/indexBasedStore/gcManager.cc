#include "indexBasedStore/gcManager.hh"
#include "indexBasedStore/configManager.hh"
#include "indexBasedStore/statsRecorder.hh"
#include <float.h>
#include <unordered_set>
#include <vector>

#define TAG_MASK (1000 * 1000)
#define RECORD_SIZE ((valueSize == INVALID_LEN ? 0 : valueSize) + (LL)sizeof(len_t) + KEY_REC_SIZE)

namespace DELTAKV_NAMESPACE {

GCManager::GCManager(KeyManager* keyManager, ValueManager* valueManager, DeviceManager* deviceManager, SegmentGroupManager* segmentGroupManager, bool isSlave)
    : _keyManager(keyManager)
    , _valueManager(valueManager)
    , _deviceManager(deviceManager)
    , _segmentGroupManager(segmentGroupManager)
{
    _maxGC = ConfigManager::getInstance().getGreedyGCSize();
    _useMmap = ConfigManager::getInstance().useMmap();
    if ((ConfigManager::getInstance().enabledVLogMode() && !_useMmap) || isSlave) {
        Segment::init(_gcSegment.read, INVALID_SEGMENT, ConfigManager::getInstance().getVLogGCSize() * 2);
    }
    Segment::init(_gcSegment.write, INVALID_SEGMENT, ConfigManager::getInstance().getVLogGCSize());
    _isSlave = isSlave;
    _gcCount.ops = 0;
    _gcCount.groups = 0;
    _gcCount.scanSize = 0;
    _gcWriteBackBytes = 0;

    //    _gcReadthreads.size_controller().resize(ConfigManager::getInstance().getNumGCReadThread());
}

GCManager::~GCManager()
{
    // for debug and measurement
    printStats();
}

void GCManager::printStats(FILE* out)
{
    fprintf(out, "_gcBytes writeBack = %lu scan = %lu\n", _gcWriteBackBytes, _gcCount.scanSize);
    fprintf(out, "Mode counts (total ops = %lu groups = %lu):\n", _gcCount.ops, _gcCount.groups);
    for (auto it : _modeCount) {
        fprintf(out, "[%d] = %lu\n", it.first, it.second);
    }
}

size_t GCManager::gcVLog()
{

    _gcCount.ops++;
    //_valueManager->_GCLock.lock();

    size_t gcBytes = 0, gcScanSize = 0;
    size_t pageSize = sysconf(_SC_PAGE_SIZE);
    len_t gcSize = ConfigManager::getInstance().getVLogGCSize();
    unsigned char* readPool = _useMmap ? 0 : Segment::getData(_gcSegment.read);
    unsigned char* writePool = Segment::getData(_gcSegment.write);
    len_t logTail = _segmentGroupManager->getLogWriteOffset();

    if (ConfigManager::getInstance().useDirectIO() && gcSize % pageSize) {
        debug_error("use direct IO but gcSize %lu not aligned; stop\n", gcSize);
        return 0;
    }

    std::vector<char*> keys;
    std::vector<ValueLocation> values;
    ValueLocation valueLoc;
    valueLoc.segmentId = 0;

    len_t capacity = ConfigManager::getInstance().getSystemEffectiveCapacity();
    len_t len = INVALID_LEN;
    offset_t logOffset = INVALID_OFFSET, gcFront = INVALID_OFFSET, flushFront = 0;
    offset_t zeroOffset = gcFront; // Fill zero content to avoid recording the length.
    len_t zeroLen = 0;

    size_t remains = 0;
    bool ret = false;
    const segment_id_t maxSegment = ConfigManager::getInstance().getNumSegment();
    key_len_t keySize;
    len_t valueSize;
    offset_t keySizeOffset;

    // counts
    len_t rewrittenKeys = 0, cleanedKeys = 0;

    _keyManager->persistMeta();

    struct timeval gcStartTime;
    gettimeofday(&gcStartTime, 0);
    debug_info("gcVLog, gcBytes %lu, gcSize %lu\n", gcBytes, gcSize);
    // scan until the designated reclaim size is reached
    while (gcBytes < gcSize) {
        // see if there is anything left over in last scan
        flushFront = Segment::getFlushFront(_gcSegment.read);
        if (flushFront == gcSize) {
            debug_error("space not enough or have bugs: remains %lu gcBytes %lu flushFront %lu logHead %lu logTail %lu\n", remains, gcBytes, flushFront, gcFront, logTail);
        }
        assert(flushFront != gcSize);
        // read and fit up only the available part of buffer
        gcFront = _segmentGroupManager->getLogGCOffset(); 
        if (zeroOffset == INVALID_OFFSET) {
            zeroOffset = gcFront;
        }

        if ((gcFront < logTail && logTail - gcFront < gcSize * 2) || 
           (gcFront > logTail && logTail + gcFront - capacity < gcSize * 2)) {
            debug_info("GC stop: go through whole log but gcBytes not enough: gcBytes %lu gcSize %lu logHead %lu logTail %lu\n", 
                gcBytes, gcSize, gcFront, logTail);
            break;
        } 

        //        debug_info("lastFillZeroOffset %lu gcFront %lu flushFront %lu\n", lastFillZeroOffset, gcFront, flushFront);
        if (_useMmap) {
            readPool = _deviceManager->readMmap(0, gcFront - flushFront, gcSize, 0);
            Segment::init(_gcSegment.read, 0, readPool, gcSize);
        } else {
            _deviceManager->readAhead(/* diskId = */ 0, gcFront + flushFront, gcSize - flushFront);
            STAT_TIME_PROCESS(_deviceManager->readDisk(/* diskId = */ 0, readPool + flushFront, gcFront + flushFront, gcSize - flushFront), StatsType::GC_READ);
        }
        // mark the amount of data in buffer
        Segment::setWriteFront(_gcSegment.read, gcSize);
        for (remains = gcSize; remains > 0;) {
            debug_trace("remains %lu gcBytes %lu flushFront %lu logHead %lu logTail %lu\n", remains, gcBytes, flushFront, gcFront, logTail);
            valueSize = INVALID_LEN;
            keySizeOffset = gcSize - remains;

            // check if we can read the key size
            if (sizeof(key_len_t) > remains) {
                debug_trace("break1: remains %lu\n", remains);
                break;
            }
            off_len_t offLen(keySizeOffset, sizeof(key_len_t));
            Segment::readData(_gcSegment.read, &keySize, offLen);
            if (keySize == 0) {
                if ((remains + (pageSize - gcFront % pageSize)) % pageSize == 0) {
                    debug_error("remains aligned; this page has no content (remains %lu)\n", remains);
                    assert(0);
                    exit(-1);
                    remains -= (flushFront == 0) ? pageSize : flushFront;
                }
                remains -= std::min(remains % pageSize + (pageSize - gcFront % pageSize), remains);
                continue;
            }

            // check if we can read the value size
            if (KEY_REC_SIZE + sizeof(len_t) > remains) {
                debug_trace("break2: keySize %lu remains %lu\n", (len_t)keySize, remains);
                break;
            }
            offLen.first = keySizeOffset + KEY_REC_SIZE;
            offLen.second = sizeof(len_t);
            Segment::readData(_gcSegment.read, &valueSize, offLen);

            // check if we can read the value. Keep it to the next buffer.
            if (KEY_REC_SIZE + sizeof(len_t) + valueSize > remains) {
                debug_trace("break3: keySize %lu [%.*s] valueSize %lu remains %lu\n", (len_t)keySize, std::min((int)keySize, 16), Segment::getData(_gcSegment.read) + keySizeOffset + sizeof(key_len_t), valueSize, remains);
                break;
            }
            STAT_TIME_PROCESS(valueLoc = _keyManager->getKey((char*)readPool + keySizeOffset), StatsType::GC_KEY_LOOKUP);
            debug_trace("read LSM: key [%.*s] offset %lu\n", (int)keySize, KEY_OFFSET((char*)readPool + keySizeOffset), valueLoc.offset);
            // check if pair is valid, avoid underflow by adding capacity, and overflow by modulation
            if (valueLoc.segmentId == (_isSlave ? maxSegment : 0) && valueLoc.offset == (gcFront + keySizeOffset + capacity) % capacity) {
                rewrittenKeys++;
                // buffer full, flush before write
                if (!Segment::canFit(_gcSegment.write, RECORD_SIZE)) {
                    // Flush to Vlog
                    STAT_TIME_PROCESS(tie(logOffset, len) = _valueManager->flushSegmentToWriteFront(_gcSegment.write, /* isGC = */ true), StatsType::GC_FLUSH);
                    assert(len > 0);
                    // update metadata
                    for (auto& v : values) {
                        v.offset = (v.offset + logOffset) % capacity;
                    }
                    STAT_TIME_PROCESS(ret = _keyManager->mergeKeyBatch(keys, values), StatsType::UPDATE_KEY_WRITE_LSM_GC);
                    if (!ret) {
                        debug_error("Failed to update %lu keys to LSM\n", keys.size());
                        assert(0);
                        exit(-1);
                    }
                    // reset keys and value holders, buffers
                    keys.clear();
                    values.clear();
                    Segment::resetFronts(_gcSegment.write);
                }
                // mark and copy the key-value pair to tbe buffer
                offset_t writeKeyOffset = Segment::getWriteFront(_gcSegment.write);
                Segment::appendData(_gcSegment.write, readPool + keySizeOffset, RECORD_SIZE);
                keys.push_back((char*)writePool + writeKeyOffset);
                valueLoc.offset = writeKeyOffset;
                valueLoc.length = valueSize;
                values.push_back(valueLoc);
                _gcWriteBackBytes += RECORD_SIZE;
            } else {
                cleanedKeys++;
                // space is reclaimed directly
                gcBytes += RECORD_SIZE;
                // cold storage invalid bytes cleared
                if (_isSlave) {
                    _valueManager->_slave.writtenBytes -= RECORD_SIZE;
                }
            }

            remains -= RECORD_SIZE;
        }
        if (remains > 0) {
            Segment::setFlushFront(_gcSegment.read, remains);
            if (!_useMmap) {
                // if some data left without scanning, move them to the front of buffer for next scan
                memmove(readPool, readPool + gcSize - remains, remains);
            }
        } else {
            // reset buffer
            Segment::resetFronts(_gcSegment.read);
        }
        if (_useMmap) {
            _deviceManager->readUmmap(0, gcFront, gcSize, readPool);
        }

        gcScanSize += gcSize;
        _segmentGroupManager->getAndIncrementVLogGCOffset(gcSize - remains);
        debug_info("one loop -- logHead %lu gcScanSize %lu (%lu keys) gcBytes %lu (%lu keys)\n", _segmentGroupManager->getLogGCOffset(), gcScanSize, rewrittenKeys + cleanedKeys, gcBytes, cleanedKeys);
    }
    // final check on remaining data to flush
    if (!keys.empty()) {

        STAT_TIME_PROCESS(tie(logOffset, len) = _valueManager->flushSegmentToWriteFront(_gcSegment.write, /* isGC */ true), StatsType::GC_FLUSH);
        for (auto& v : values) {
            v.offset = (v.offset + logOffset) % capacity;
        }
        // update metadata of flushed data
        STAT_TIME_PROCESS(ret = _keyManager->mergeKeyBatch(keys, values), StatsType::UPDATE_KEY_WRITE_LSM_GC);
        if (!ret) {
            debug_error("Failed to update %lu keys to LSM\n", keys.size());
            assert(0);
            exit(-1);
        }

        if (ConfigManager::getInstance().persistLogMeta()) {
            std::string value;
            value.append(to_string(logOffset + len));
            _keyManager->writeMeta(SegmentGroupManager::LogTailString, strlen(SegmentGroupManager::LogTailString), value);
            _keyManager->persistMeta();
        }
    }
    // printf("gcBytes %lu\n", gcBytes);
    StatsRecorder::getInstance()->totalProcess(StatsType::GC_SCAN_BYTES, gcScanSize);
    StatsRecorder::getInstance()->totalProcess(StatsType::GC_WRITE_BYTES, gcScanSize - gcBytes);
    // reset write buffer
    Segment::resetFronts(_gcSegment.write);

    debug_info("after GC, logHead %lu gcScanSize %lu cleaned bytes %lu\n", _segmentGroupManager->getLogGCOffset(), gcScanSize, gcBytes);

    if (ConfigManager::getInstance().persistLogMeta()) {
        std::string value;
        value.append(to_string(gcFront + gcSize - remains));
        _keyManager->writeMeta(SegmentGroupManager::LogHeadString, strlen(SegmentGroupManager::LogHeadString), value);
    }
    Segment::resetFronts(_gcSegment.read);
    StatsRecorder::getInstance()->timeProcess(StatsType::GC_TOTAL, gcStartTime);

    _gcCount.scanSize += gcScanSize;
    //_valueManager->_GCLock.unlock();
    return gcBytes;
}

inline int GCManager::getHotness(group_id_t groupId, int updateCount)
{
    return (updateCount > 1) ? ConfigManager::getInstance().getHotnessLevel() / 2 : 0;
}

GCMode GCManager::getGCMode(group_id_t groupId, len_t reservedBytes)
{
    GCMode gcMode = ConfigManager::getInstance().getGCMode();
    // check if the triggering condition(s) matches,
    // swtich to all if
    // (1) log reclaim is less than reclaim min threshold
    // (2) write back ratio is smaller for all
    double ratioAll = _segmentGroupManager->getGroupWriteBackRatio(groupId, /* type = */ 0, /* isGC = */ true);
    double ratioLogOnly = _segmentGroupManager->getGroupWriteBackRatio(groupId, /* type = */ 1, /* isGC = */ true);
    if (gcMode == LOG_ONLY && ratioAll <= ratioLogOnly) {
        gcMode = ALL;
    }
    return gcMode;
}

}

#undef TAG_MASK
