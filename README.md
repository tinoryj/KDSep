# KDSep: Key-Delta Separation

This is the code repository for the submitted Eurosys'24 paper: "Enhancing LSM-tree Key-Value Stores for Read-Modify-Writes via Key-Delta Separation". 

You are recommended to run the system on Ubuntu 22.04 to support `liburing-dev`. Make sure you install:

```shell
$ sudo apt-get install libboost-all-dev clang llvm libzstd-dev liblz4-dev libsnappy-dev liburing-dev
```

Go to the directory `Simulator/YCSB-C` to compile:

```shell
$ cd Simulator/YCSB-C
$ scripts/make_release.sh
```

Run the experiments:

```shell
$ scripts/test.sh
```
