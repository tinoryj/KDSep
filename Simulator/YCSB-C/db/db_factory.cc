//
//  basic_db.cc
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "db/db_factory.h"
#include "LevelDB/leveldb_db.h"
#include "RocksDB/rocksdb_db.h"
#include <string>

using namespace std;
using ycsbc::DB;
using ycsbc::DBFactory;

DB* DBFactory::CreateDB(utils::Properties& props)
{
    if (props["dbname"] == "leveldb") {
        return new LevelDB(props["dbfilename"].c_str(), props["configpath"]);
    } else if (props["dbname"] == "rocksdb" || props["dbname"] == "rocksdb_tiered") {
        return new RocksDB(props["dbfilename"].c_str(), props["configpath"]);
    } else
        return NULL;
}
