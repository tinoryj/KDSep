CC=g++
CFLAGS=-std=c++17 -c -g -Wall
INCLUDES=-I ../ -I $(shell pwd)/../../Databases/LevelDB/include/
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

