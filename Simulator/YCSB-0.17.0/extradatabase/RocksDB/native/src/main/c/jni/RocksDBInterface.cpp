#include "rocksdb/db.h"
#include <cassert>
#include <cstdio>
#include <jni.h>
#include <string>

#ifdef __cplusplus
extern "C" {
#endif

using namespace std;

JNIEXPORT void JNICALL Java_RocksDBInterface_show(JNIEnv* jenv, jclass jcls)
{
    printf("Goodbye World!\n");
}

JNIEXPORT jboolean JNICALL Java_RocksDBInterface_initDB(JNIEnv* jenv, jclass jcls, jstring jDBPath)
{
    // const auto* databasePathChar = jenv->GetStringUTFChars(jDBPath, nullptr);
    string databasePathStr = "test";
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, databasePathStr, &db);
    if (status.ok()) {
        return true;
    } else {
        return false;
    }
}

#ifdef __cplusplus
}
#endif