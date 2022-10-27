//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Enum PrepopulateDeltaCache
 *
 * <p>
 * Prepopulate warm/hot deltas which are already in memory into delta
 * cache at the time of flush. On a flush, the delta that is in memory
 * (in memtables) get flushed to the device. If using Direct IO,
 * additional IO is incurred to read this delta back into memory again,
 * which is avoided by enabling this option. This further helps if the
 * workload exhibits high temporal locality, where most of the reads go
 * to recently written data. This also helps in case of the remote file
 * system since it involves network traffic and higher latencies.
 * </p>
 */
public enum PrepopulateDeltaCache {
  PREPOPULATE_DELTA_DISABLE((byte) 0x0, "prepopulate_delta_disable", "kDisable"),
  PREPOPULATE_DELTA_FLUSH_ONLY((byte) 0x1, "prepopulate_delta_flush_only", "kFlushOnly");

  /**
   * <p>
   * Get the PrepopulateDeltaCache enumeration value by
   * passing the library name to this method.
   * </p>
   *
   * <p>
   * If library cannot be found the enumeration
   * value {@code PREPOPULATE_DELTA_DISABLE} will be returned.
   * </p>
   *
   * @param libraryName prepopulate delta cache library name.
   *
   * @return PrepopulateDeltaCache instance.
   */
  public static PrepopulateDeltaCache getPrepopulateDeltaCache(String libraryName) {
    if (libraryName != null) {
      for (PrepopulateDeltaCache prepopulateDeltaCache : PrepopulateDeltaCache.values()) {
        if (prepopulateDeltaCache.getLibraryName() != null
            && prepopulateDeltaCache.getLibraryName().equals(libraryName)) {
          return prepopulateDeltaCache;
        }
      }
    }
    return PrepopulateDeltaCache.PREPOPULATE_DELTA_DISABLE;
  }

  /**
   * <p>
   * Get the PrepopulateDeltaCache enumeration value by
   * passing the byte identifier to this method.
   * </p>
   *
   * @param byteIdentifier of PrepopulateDeltaCache.
   *
   * @return PrepopulateDeltaCache instance.
   *
   * @throws IllegalArgumentException If PrepopulateDeltaCache cannot be found for
   *                                  the
   *                                  provided byteIdentifier
   */
  public static PrepopulateDeltaCache getPrepopulateDeltaCache(byte byteIdentifier) {
    for (final PrepopulateDeltaCache prepopulateDeltaCache : PrepopulateDeltaCache.values()) {
      if (prepopulateDeltaCache.getValue() == byteIdentifier) {
        return prepopulateDeltaCache;
      }
    }

    throw new IllegalArgumentException("Illegal value provided for PrepopulateDeltaCache.");
  }

  /**
   * <p>
   * Get a PrepopulateDeltaCache value based on the string key in the C++ options
   * output.
   * This gets used in support of getting options into Java from an options
   * string,
   * which is generated at the C++ level.
   * </p>
   *
   * @param internalName the internal (C++) name by which the option is known.
   *
   * @return PrepopulateDeltaCache instance (optional)
   */
  static PrepopulateDeltaCache getFromInternal(final String internalName) {
    for (final PrepopulateDeltaCache prepopulateDeltaCache : PrepopulateDeltaCache.values()) {
      if (prepopulateDeltaCache.internalName_.equals(internalName)) {
        return prepopulateDeltaCache;
      }
    }

    throw new IllegalArgumentException(
        "Illegal internalName '" + internalName + " ' provided for PrepopulateDeltaCache.");
  }

  /**
   * <p>
   * Returns the byte value of the enumerations value.
   * </p>
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  /**
   * <p>
   * Returns the library name of the prepopulate delta cache mode
   * identified by the enumeration value.
   * </p>
   *
   * @return library name
   */
  public String getLibraryName() {
    return libraryName_;
  }

  PrepopulateDeltaCache(final byte value, final String libraryName, final String internalName) {
    value_ = value;
    libraryName_ = libraryName;
    internalName_ = internalName;
  }

  private final byte value_;
  private final String libraryName_;
  private final String internalName_;
}
