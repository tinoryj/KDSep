# Notes

1. Compile RocksDB static library via `make  static_lib  EXTRA_CXXFLAGS=-fPIC EXTRA_CFLAGS=-fPIC USE_RTTI=1 DEBUG_LEVEL=0` to avoid inherent errors when link the static library.