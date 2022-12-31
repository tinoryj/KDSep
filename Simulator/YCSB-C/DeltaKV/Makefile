CC=icpx
CFLAGS=-O3 -std=c++17 -c -g
INCLUDES=-I ./ -I ../ -I $(shell pwd)/../../../RocksDBAddons/include/ -I $(shell pwd)/../../../rocksdb-7.7.3/include/
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

