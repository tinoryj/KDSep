#ifndef __KEYVALUE_HH__
#define __KEYVALUE_HH__

#include "common/dataStructure.hpp"
#include "common/indexStorePreDefines.hpp"
#include "indexBasedStore/configManager.hh"
#include "utils/hash.hpp"
#include <stdlib.h>

#define LSM_MASK (0x80000000)

namespace DELTAKV_NAMESPACE {

struct equalKey {
    bool operator()(unsigned char* const& a, unsigned char* const& b) const
    {
        key_len_t keySizeA, keySizeB;
        memcpy(&keySizeA, a, sizeof(key_len_t));
        memcpy(&keySizeB, b, sizeof(key_len_t));

        if (keySizeA != keySizeB) {
            key_len_t keySizeMin = keySizeA > keySizeB ? keySizeB : keySizeA;
            int ret = memcmp(a, b, keySizeMin);
            if (ret == 0) {
                if (keySizeA > keySizeB)
                    return 1;
                return -1;
            }
            return ret;
        }
        return (memcmp(a, b, keySizeA) == 0);
    }
};

struct hashKey {
    size_t operator()(unsigned char* const& s) const
    {
        key_len_t keySize;
        memcpy(&keySize, s, sizeof(key_len_t));
        return HashFunc::hash((char*)s + sizeof(key_len_t), (int)keySize);
        // return std::hash<unsigned char>{}(*s);
    }
};

/*
class KeyValueRecord {
public:
    KeyValueRecord() {
        _keySize = _valueSize = 0;
        _key = _value = nullptr;
        _paddingSize = 0;
        _totalSize = 0;
    }

    bool read(char* recordOffset, len_t maxSize = INVALID_LEN) {
        if (sizeof(key_len_t) > maxSize) {
            return false;
        }
        memcpy(&_keySize, recordOffset, sizeof(key_len_t));

        if (sizeof(key_len_t) + _keySize > maxSize) {
            return false;
        }
        _key = recordOffset + sizeof(key_len_t);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) > maxSize) {
            return false;
        }
        memcpy(&_valueSize, recordOffset + sizeof(key_len_t) + _keySize);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) + _valueSize > maxSize) {
            return false;
        }
        _value = recordOffset + sizeof(key_len_t) + _keySize + sizeof(len_t);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) * 2 + _valueSize > maxSize) {
            return true;
        }
        memcpy(&_paddingSize, recordOffset + sizeof(key_len_t) + _keySize + sizeof(len_t) + _valueSize, sizeof(len_t));
        return true;
    }

    bool write(char* recordOffset, len_t maxSize = INVALID_LEN) {
        if (sizeof(key_len_t) > maxSize) {
            return false;
        }
        memcpy(recordOffset, &_keySize, sizeof(key_len_t));
        recordOffset += sizeof(key_len_t);

        if (sizeof(key_len_t) + _keySize > maxSize) {
            return false;
        }
        memcpy(recordOffset, _key, );
        _key = recordOffset + sizeof(key_len_t);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) > maxSize) {
            return false;
        }
        memcpy(&_valueSize, recordOffset + sizeof(key_len_t) + _keySize);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) + _valueSize > maxSize) {
            return false;
        }
        _value = recordOffset + sizeof(key_len_t) + _keySize + sizeof(len_t);

        if (sizeof(key_len_t) + _keySize + sizeof(len_t) * 2 + _valueSize > maxSize) {
            return true;
        }
        memcpy(&_paddingSize, recordOffset + sizeof(key_len_t) + _keySize + sizeof(len_t) + _valueSize, sizeof(len_t));
        return true;
    }

    char* getKey() {
        return _key;
    }

    char* getValue() {
        return _value;
    }

    key_len_t getKeySize() {
        return _keySize;
    }

    len_t getValueSize() {
        return _valueSize;
    }

    len_t getPaddingSize() {
        return _paddingSize;
    }

private:
    key_len_t _keySize;
    len_t _valueSize;
    char* _key;
    char* _value;
    len_t _paddingSize;
    len_t _totalSize;
}
*/

class ValueLocation {
public:
    segment_len_t length;
    segment_id_t segmentId;
    segment_offset_t offset;
    std::string value;

    ValueLocation()
    {
        segmentId = INVALID_SEGMENT;
        offset = INVALID_OFFSET;
        length = INVALID_LEN;
    }

    ~ValueLocation() { }

    static unsigned int size()
    {
        return sizeof(segment_id_t) + sizeof(segment_offset_t) + sizeof(segment_len_t);
    }

    std::string serialize()
    {
        bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
        bool vlog = ConfigManager::getInstance().enabledVLogMode();
        len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
        len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
        segment_id_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();

        std::string str;
        // write length
        len_t flength = this->length;
        if (this->segmentId == LSM_SEGMENT) {
            flength |= LSM_MASK;
        }
        str.append((char*)&flength, sizeof(this->length));
        // write segment id (if kv-separation is enabled)
        offset_t foffset = this->offset;
        if (!disableKvSep && !vlog) {
            if (this->segmentId == LSM_SEGMENT) {
                foffset = INVALID_OFFSET;
            } else if (this->segmentId < numMainSegment) {
                foffset = this->segmentId * mainSegmentSize + this->offset;
            } else {
                foffset = mainSegmentSize * numMainSegment + (this->segmentId - numMainSegment) * logSegmentSize + this->offset;
            }
            // str.append((char*) &this->segmentId, sizeof(this->segmentId));
        } else if (!vlog) {
            this->segmentId = LSM_SEGMENT;
        }
        // write value or offset
        if (this->segmentId == LSM_SEGMENT) {
            str.append(value);
        } else {
            str.append((char*)&foffset, sizeof(this->offset));
        }
        return str;
    }

    std::string serializeIndexUpdate()
    {
        bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
        bool vlog = ConfigManager::getInstance().enabledVLogMode();
        len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
        len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
        segment_id_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();
        char str[sizeof(internalValueType) + sizeof(externalIndexInfo) + 1];
        internalValueType valueType;
        externalIndexInfo indexInfo;
        valueType.mergeFlag_ = valueType.valueSeparatedFlag_ = true;

        // write length
        len_t flength = this->length;
        if (this->segmentId == LSM_SEGMENT) {
            flength |= LSM_MASK;
        }
        // write segment id (if kv-separation is enabled)
        offset_t foffset = this->offset;
        if (!disableKvSep && !vlog) {
            if (this->segmentId == LSM_SEGMENT) {
                foffset = INVALID_OFFSET;
            } else if (this->segmentId < numMainSegment) {
                foffset = this->segmentId * mainSegmentSize + this->offset;
            } else {
                foffset = mainSegmentSize * numMainSegment + (this->segmentId - numMainSegment) * logSegmentSize + this->offset;
            }
            // str.append((char*) &this->segmentId, sizeof(this->segmentId));
        } else if (!vlog) {
            this->segmentId = LSM_SEGMENT;
        }
        // write value or offset

        valueType.rawValueSize_ = flength;
        indexInfo.externalFileID_ = 0;
        indexInfo.externalFileOffset_ = foffset;
        indexInfo.externalContentSize_ = flength;

        memcpy(str, &valueType, sizeof(internalValueType));
        memcpy(str + sizeof(internalValueType), &indexInfo, sizeof(externalIndexInfo));

        //        str.append((char*) &flength, sizeof(this->length));
        //        if (this->segmentId == LSM_SEGMENT) {
        //            str.append(value);
        //        } else {
        //            str.append((char*) &foffset, sizeof(this->offset));
        //        }
        return std::string(str, sizeof(internalValueType) + sizeof(externalIndexInfo));
    }

    bool deserialize(std::string str)
    {
        bool disableKvSep = ConfigManager::getInstance().disableKvSeparation();
        bool vlog = ConfigManager::getInstance().enabledVLogMode();
        len_t mainSegmentSize = ConfigManager::getInstance().getMainSegmentSize();
        len_t logSegmentSize = ConfigManager::getInstance().getLogSegmentSize();
        segment_id_t numMainSegment = ConfigManager::getInstance().getNumMainSegment();
        segment_id_t numLogSegment = ConfigManager::getInstance().getNumLogSegment();

        const char* cstr = str.c_str();
        size_t offset = 0;

        externalIndexInfo indexInfo;
        if (str.length() < sizeof(internalValueType) + sizeof(externalIndexInfo)) {
            return false;
        }
        memcpy(&indexInfo, cstr + sizeof(internalValueType), sizeof(externalIndexInfo));

        // read length
        //        memcpy(&this->length, cstr + offset, sizeof(this->length));
        this->length = indexInfo.externalContentSize_;
        this->offset = indexInfo.externalFileOffset_;

        offset += sizeof(this->length);
        if (this->length & LSM_MASK) {
            this->segmentId = LSM_SEGMENT;
            this->length ^= LSM_MASK;
        }
        // read segment id (if kv-separation is enabled)
        if (!disableKvSep && !vlog) {
            // memcpy(&this->segmentId, cstr + offset, sizeof(this->segmentId));
            // offset += sizeof(this->segmentId);
        } else if (!vlog) {
            this->segmentId = LSM_SEGMENT;
        } else {
            this->segmentId = 0;
        }
        // read value or offset
        if (this->segmentId == LSM_SEGMENT) {
            value.assign(cstr + offset, this->length);
        } else {
            //            memcpy(&this->offset, cstr + offset, sizeof(this->offset));
            if (!vlog) {
                if (this->offset < numMainSegment * mainSegmentSize) {
                    this->segmentId = this->offset / mainSegmentSize;
                    this->offset %= mainSegmentSize;
                } else if (this->offset - mainSegmentSize * numMainSegment > logSegmentSize * numLogSegment) {
                    // appended cold storage
                    this->segmentId = numMainSegment + numLogSegment;
                    this->offset = this->offset - (mainSegmentSize * numMainSegment + numLogSegment * logSegmentSize);
                } else {
                    this->segmentId = (this->offset - mainSegmentSize * numMainSegment) / logSegmentSize;
                    this->offset = this->offset - (mainSegmentSize * numMainSegment + this->segmentId * logSegmentSize);
                    this->segmentId += numMainSegment;
                }
            }
        }
        return true;
    }

    inline bool operator==(ValueLocation& vl)
    {
        bool ret = false;
        if (
            (this->segmentId == LSM_SEGMENT && vl.segmentId == LSM_SEGMENT) || ConfigManager::getInstance().disableKvSeparation()) {
            ret = (this->length == vl.length && this->value == vl.value);
        } else {
            ret = (this->segmentId == vl.segmentId && this->offset == vl.offset && this->length == vl.length);
        }
        return ret;
    }
};

}

#endif
