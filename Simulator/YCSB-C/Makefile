CC=g++
SEARCH_LIBRARY_ROCKSDB=$(shell pwd)/../../rocksdb-7.7.3/include/
# SEARCH_LIBRARY_LEVELDB=$(shell pwd)/../Databases/LevelDB/include/
LD_LIBRARY_ROCKSDB=$(shell pwd)/../../rocksdb-7.7.3/librocksdb.a
# LD_LIBRARY_LEVELDB=$(shell pwd)/../Databases/LevelDB/libleveldb.a
INCLUDES= -I ./ -I $(SEARCH_LIBRARY_ROCKSDB) #-I $(SEARCH_LIBRARY_LEVELDB)
CFLAGS=-O3 -std=c++17 -g -Wall -pthread -ggdb -fsanitize=address
LDFLAGS=$(LD_LIBRARY_ROCKSDB) -lzstd -lpthread -llz4 -lbz2 -lsnappy -lz -ldl -luring #$(LD_LIBRARY_LEVELDB) 
SUBDIRS=core db RocksDB #LevelDB 
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc) $(wildcard RocksDB/*.cc) # $(wildcard LevelDB/*.cc)
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



