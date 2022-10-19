//
// Created by wujy on 1/23/19.
//

#ifndef YCSB_C_ROCKSDB_DB_H
#define YCSB_C_ROCKSDB_DB_H

#include "core/db.h"
#include "core/properties.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/trace_reader_writer.h"
#include "utilities/merge_operators.h"
#include <iostream>
#include <string>

using std::cout;
using std::endl;

namespace ycsbc {
class RocksDB : public DB {
public:
    RocksDB(const char* dbfilename, const std::string& config_file_path);
    int Read(const std::string& table, const std::string& key,
        const std::vector<std::string>* fields,
        std::vector<KVPair>& result);

    int Scan(const std::string& table, const std::string& key,
        int len, const std::vector<std::string>* fields,
        std::vector<std::vector<KVPair>>& result);

    int Insert(const std::string& table, const std::string& key,
        std::vector<KVPair>& values);

    int Update(const std::string& table, const std::string& key,
        std::vector<KVPair>& values);

    int Delete(const std::string& table, const std::string& key);

    void printStats();

    ~RocksDB();

private:
    rocksdb::DB* db_;
    unsigned noResult;
    rocksdb::Iterator* it { nullptr };
};
}

#endif //YCSB_C_ROCKSDB_DB_H
