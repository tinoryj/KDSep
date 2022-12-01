#!/bin/bash
if [ ! -d "/opt/rocksdb-7.7.3" ]; then
    echo "Not found rocksdb-7.7.3 in /opt"
    if [ ! -f "../rocksdb-7.7.3/librocksdb.a" ]; then
        echo "Not found librocksdb.a in ../rocksdb-7.7.3 start build"
        cd ../rocksdb-7.7.3 || exit
        make clean # clean up current build result since it may have errors
        make static_lib EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC USE_RTTI=1 DEBUG_LEVEL=0 -j16
        cd ../RocksDBAddons || exit
    fi
    echo "Copy rocksdb static lib to /opt"
    sudo mkdir -p /opt/rocksdb-7.7.3
    sudo cp -r ../rocksdb-7.7.3/include /opt/rocksdb-7.7.3
    sudo cp ../rocksdb-7.7.3/librocksdb.a /opt/rocksdb-7.7.3
fi

./scripts/cleanup.sh

cd ./build || exit
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(shell grep -c ^processor /proc/cpuinfo 2>/dev/null)
cd .. || exit

if [ ! -f "bin/test" ]; then
    echo -e "\033[31mBuild error, exit without testing \033[0m"
else
    echo -e "\n"
    ulimit -n 65536
    echo "Local Test with simple operations (Round 1) ===>"
    bin/test 1000
    echo "Local Test with simple operations (Round 1) <==="
    echo "Local Test with simple operations (Round 2) ===>"
    bin/test 1
    echo "Local Test with simple operations (Round 2) <==="
fi
