CC=g++
SEARCH_LIBRARY_ROCKSDB=$(shell pwd)/../../rocksdb-7.7.3/include/
SEARCH_LIBRARY_DELTAKV=$(shell pwd)/../../RocksDBAddons/include/
# SEARCH_LIBRARY_LEVELDB=$(shell pwd)/../Databases/LevelDB/include/
LD_LIBRARY_ROCKSDB=$(shell pwd)/../../rocksdb-7.7.3/librocksdb.a 
LD_LIBRARY_DELTAKV=$(shell pwd)/../../RocksDBAddons/lib/libDeltaKV.a
# LD_LIBRARY_LEVELDB=$(shell pwd)/../Databases/LevelDB/libleveldb.a
INCLUDES= -I ./ -I $(SEARCH_LIBRARY_ROCKSDB) -I ${SEARCH_LIBRARY_DELTAKV} #-I $(SEARCH_LIBRARY_LEVELDB)
CFLAGS=-O3 -std=c++17 -g -Wall -pthread -ggdb -fsanitize=address
LDFLAGS=$(LD_LIBRARY_ROCKSDB) $(LD_LIBRARY_DELTAKV) -lzstd -lpthread -llz4 -lbz2 -lsnappy -lz -ldl -luring -lboost_thread -lboost_system -lboost_serialization #$(LD_LIBRARY_LEVELDB) 
SUBDIRS=core db DeltaKV #RocksDB #LevelDB 
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc) $(wildcard DeltaKV/*.cc) # $(wildcard RocksDB/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)
.PHONY: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $(INCLUDES) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
		done
	$(RM) $(EXEC)
	rm -rf ./dbtest



