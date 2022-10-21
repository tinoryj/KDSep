CC=g++
SEARCH_LIBRARY_ROCKSDB=$(shell pwd)/../Databases/RocksDB/include/
SEARCH_LIBRARY_LEVELDB=$(shell pwd)/../Databases/LevelDB/include/
LD_LIBRARY_ROCKSDB=$(shell pwd)/../Databases/RocksDB/librocksdb.a
LD_LIBRARY_LEVELDB=$(shell pwd)/../Databases/LevelDB/libleveldb.a
INCLUDES= -I ./ -I $(SEARCH_LIBRARY_ROCKSDB) -I $(SEARCH_LIBRARY_LEVELDB)
CFLAGS=-O3 -std=c++17 -g -Wall -pthread #-ggdb -fsanitize=address
LDFLAGS=$(LD_LIBRARY_ROCKSDB) $(LD_LIBRARY_LEVELDB) -lzstd -lpthread -llz4 -lbz2 -lsnappy -lz -ldl -lrocksdb
SUBDIRS=RocksDB LevelDB core db 
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc) $(wildcard RocksDB/*.cc) $(wildcard LevelDB/*.cc)
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
	# rm -rf ./dbtest



