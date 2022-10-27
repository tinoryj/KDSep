// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FilenameFilter;
import java.util.*;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DeltaOptionsTest {
    @ClassRule
    public static final RocksNativeLibraryResource ROCKS_NATIVE_LIBRARY_RESOURCE = new RocksNativeLibraryResource();

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    final int minDeltaSize = 65536;
    final int largeDeltaSize = 65536 * 2;

    /**
     * Count the files in the temporary folder which end with a particular suffix
     * Used to query the state of a test database to check if it is as the test
     * expects
     *
     * @param endsWith the suffix to match
     * @return the number of files with a matching suffix
     */
    @SuppressWarnings("CallToStringConcatCanBeReplacedByOperator")
    private int countDBFiles(final String endsWith) {
        return Objects
                .requireNonNull(dbFolder.getRoot().list(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(endsWith);
                    }
                })).length;
    }

    @SuppressWarnings("SameParameterValue")
    private byte[] small_key(String suffix) {
        return ("small_key_" + suffix).getBytes(UTF_8);
    }

    @SuppressWarnings("SameParameterValue")
    private byte[] small_value(String suffix) {
        return ("small_value_" + suffix).getBytes(UTF_8);
    }

    private byte[] large_key(String suffix) {
        return ("large_key_" + suffix).getBytes(UTF_8);
    }

    private byte[] large_value(String repeat) {
        final byte[] large_value = ("" + repeat + "_" + largeDeltaSize + "b").getBytes(UTF_8);
        final byte[] large_buffer = new byte[largeDeltaSize];
        for (int pos = 0; pos < largeDeltaSize; pos += large_value.length) {
            int numBytes = Math.min(large_value.length, large_buffer.length - pos);
            System.arraycopy(large_value, 0, large_buffer, pos, numBytes);
        }
        return large_buffer;
    }

    @Test
    public void deltaOptions() {
        try (final Options options = new Options()) {
            assertThat(options.enableDeltaFiles()).isEqualTo(false);
            assertThat(options.minDeltaSize()).isEqualTo(0);
            assertThat(options.deltaCompressionType()).isEqualTo(CompressionType.NO_COMPRESSION);
            assertThat(options.enableDeltaGarbageCollection()).isEqualTo(false);
            assertThat(options.deltaFileSize()).isEqualTo(268435456L);
            assertThat(options.deltaGarbageCollectionAgeCutoff()).isEqualTo(0.25);
            assertThat(options.deltaGarbageCollectionForceThreshold()).isEqualTo(1.0);
            assertThat(options.deltaCompactionReadaheadSize()).isEqualTo(0);
            assertThat(options.prepopulateDeltaCache())
                    .isEqualTo(PrepopulateDeltaCache.PREPOPULATE_DELTA_DISABLE);

            assertThat(options.setEnableDeltaFiles(true)).isEqualTo(options);
            assertThat(options.setMinDeltaSize(132768L)).isEqualTo(options);
            assertThat(options.setDeltaCompressionType(CompressionType.BZLIB2_COMPRESSION))
                    .isEqualTo(options);
            assertThat(options.setEnableDeltaGarbageCollection(true)).isEqualTo(options);
            assertThat(options.setDeltaFileSize(132768L)).isEqualTo(options);
            assertThat(options.setDeltaGarbageCollectionAgeCutoff(0.89)).isEqualTo(options);
            assertThat(options.setDeltaGarbageCollectionForceThreshold(0.80)).isEqualTo(options);
            assertThat(options.setDeltaCompactionReadaheadSize(262144L)).isEqualTo(options);
            assertThat(options.setDeltaFileStartingLevel(0)).isEqualTo(options);
            assertThat(options.setPrepopulateDeltaCache(PrepopulateDeltaCache.PREPOPULATE_DELTA_FLUSH_ONLY))
                    .isEqualTo(options);

            assertThat(options.enableDeltaFiles()).isEqualTo(true);
            assertThat(options.minDeltaSize()).isEqualTo(132768L);
            assertThat(options.deltaCompressionType()).isEqualTo(CompressionType.BZLIB2_COMPRESSION);
            assertThat(options.enableDeltaGarbageCollection()).isEqualTo(true);
            assertThat(options.deltaFileSize()).isEqualTo(132768L);
            assertThat(options.deltaGarbageCollectionAgeCutoff()).isEqualTo(0.89);
            assertThat(options.deltaGarbageCollectionForceThreshold()).isEqualTo(0.80);
            assertThat(options.deltaCompactionReadaheadSize()).isEqualTo(262144L);
            assertThat(options.deltaFileStartingLevel()).isEqualTo(0);
            assertThat(options.prepopulateDeltaCache())
                    .isEqualTo(PrepopulateDeltaCache.PREPOPULATE_DELTA_FLUSH_ONLY);
        }
    }

    @Test
    public void deltaColumnFamilyOptions() {
        try (final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()) {
            assertThat(columnFamilyOptions.enableDeltaFiles()).isEqualTo(false);
            assertThat(columnFamilyOptions.minDeltaSize()).isEqualTo(0);
            assertThat(columnFamilyOptions.deltaCompressionType())
                    .isEqualTo(CompressionType.NO_COMPRESSION);
            assertThat(columnFamilyOptions.enableDeltaGarbageCollection()).isEqualTo(false);
            assertThat(columnFamilyOptions.deltaFileSize()).isEqualTo(268435456L);
            assertThat(columnFamilyOptions.deltaGarbageCollectionAgeCutoff()).isEqualTo(0.25);
            assertThat(columnFamilyOptions.deltaGarbageCollectionForceThreshold()).isEqualTo(1.0);
            assertThat(columnFamilyOptions.deltaCompactionReadaheadSize()).isEqualTo(0);

            assertThat(columnFamilyOptions.setEnableDeltaFiles(true)).isEqualTo(columnFamilyOptions);
            assertThat(columnFamilyOptions.setMinDeltaSize(132768L)).isEqualTo(columnFamilyOptions);
            assertThat(columnFamilyOptions.setDeltaCompressionType(CompressionType.BZLIB2_COMPRESSION))
                    .isEqualTo(columnFamilyOptions);
            assertThat(columnFamilyOptions.setEnableDeltaGarbageCollection(true))
                    .isEqualTo(columnFamilyOptions);
            assertThat(columnFamilyOptions.setDeltaFileSize(132768L)).isEqualTo(columnFamilyOptions);
            assertThat(columnFamilyOptions.setDeltaGarbageCollectionAgeCutoff(0.89))
                    .isEqualTo(columnFamilyOptions);
            assertThat(columnFamilyOptions.setDeltaGarbageCollectionForceThreshold(0.80))
                    .isEqualTo(columnFamilyOptions);
            assertThat(columnFamilyOptions.setDeltaCompactionReadaheadSize(262144L))
                    .isEqualTo(columnFamilyOptions);
            assertThat(columnFamilyOptions.setDeltaFileStartingLevel(0)).isEqualTo(columnFamilyOptions);
            assertThat(columnFamilyOptions.setPrepopulateDeltaCache(
                    PrepopulateDeltaCache.PREPOPULATE_DELTA_DISABLE))
                    .isEqualTo(columnFamilyOptions);

            assertThat(columnFamilyOptions.enableDeltaFiles()).isEqualTo(true);
            assertThat(columnFamilyOptions.minDeltaSize()).isEqualTo(132768L);
            assertThat(columnFamilyOptions.deltaCompressionType())
                    .isEqualTo(CompressionType.BZLIB2_COMPRESSION);
            assertThat(columnFamilyOptions.enableDeltaGarbageCollection()).isEqualTo(true);
            assertThat(columnFamilyOptions.deltaFileSize()).isEqualTo(132768L);
            assertThat(columnFamilyOptions.deltaGarbageCollectionAgeCutoff()).isEqualTo(0.89);
            assertThat(columnFamilyOptions.deltaGarbageCollectionForceThreshold()).isEqualTo(0.80);
            assertThat(columnFamilyOptions.deltaCompactionReadaheadSize()).isEqualTo(262144L);
            assertThat(columnFamilyOptions.deltaFileStartingLevel()).isEqualTo(0);
            assertThat(columnFamilyOptions.prepopulateDeltaCache())
                    .isEqualTo(PrepopulateDeltaCache.PREPOPULATE_DELTA_DISABLE);
        }
    }

    @Test
    public void deltaMutableColumnFamilyOptionsBuilder() {
        final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder = MutableColumnFamilyOptions
                .builder();
        builder.setEnableDeltaFiles(true)
                .setMinDeltaSize(1024)
                .setDeltaFileSize(132768)
                .setDeltaCompressionType(CompressionType.BZLIB2_COMPRESSION)
                .setEnableDeltaGarbageCollection(true)
                .setDeltaGarbageCollectionAgeCutoff(0.89)
                .setDeltaGarbageCollectionForceThreshold(0.80)
                .setDeltaCompactionReadaheadSize(262144)
                .setDeltaFileStartingLevel(1)
                .setPrepopulateDeltaCache(PrepopulateDeltaCache.PREPOPULATE_DELTA_FLUSH_ONLY);

        assertThat(builder.enableDeltaFiles()).isEqualTo(true);
        assertThat(builder.minDeltaSize()).isEqualTo(1024);
        assertThat(builder.deltaFileSize()).isEqualTo(132768);
        assertThat(builder.deltaCompressionType()).isEqualTo(CompressionType.BZLIB2_COMPRESSION);
        assertThat(builder.enableDeltaGarbageCollection()).isEqualTo(true);
        assertThat(builder.deltaGarbageCollectionAgeCutoff()).isEqualTo(0.89);
        assertThat(builder.deltaGarbageCollectionForceThreshold()).isEqualTo(0.80);
        assertThat(builder.deltaCompactionReadaheadSize()).isEqualTo(262144);
        assertThat(builder.deltaFileStartingLevel()).isEqualTo(1);
        assertThat(builder.prepopulateDeltaCache())
                .isEqualTo(PrepopulateDeltaCache.PREPOPULATE_DELTA_FLUSH_ONLY);

        builder.setEnableDeltaFiles(false)
                .setMinDeltaSize(4096)
                .setDeltaFileSize(2048)
                .setDeltaCompressionType(CompressionType.LZ4_COMPRESSION)
                .setEnableDeltaGarbageCollection(false)
                .setDeltaGarbageCollectionAgeCutoff(0.91)
                .setDeltaGarbageCollectionForceThreshold(0.96)
                .setDeltaCompactionReadaheadSize(1024)
                .setDeltaFileStartingLevel(0)
                .setPrepopulateDeltaCache(PrepopulateDeltaCache.PREPOPULATE_DELTA_DISABLE);

        assertThat(builder.enableDeltaFiles()).isEqualTo(false);
        assertThat(builder.minDeltaSize()).isEqualTo(4096);
        assertThat(builder.deltaFileSize()).isEqualTo(2048);
        assertThat(builder.deltaCompressionType()).isEqualTo(CompressionType.LZ4_COMPRESSION);
        assertThat(builder.enableDeltaGarbageCollection()).isEqualTo(false);
        assertThat(builder.deltaGarbageCollectionAgeCutoff()).isEqualTo(0.91);
        assertThat(builder.deltaGarbageCollectionForceThreshold()).isEqualTo(0.96);
        assertThat(builder.deltaCompactionReadaheadSize()).isEqualTo(1024);
        assertThat(builder.deltaFileStartingLevel()).isEqualTo(0);
        assertThat(builder.prepopulateDeltaCache())
                .isEqualTo(PrepopulateDeltaCache.PREPOPULATE_DELTA_DISABLE);

        final MutableColumnFamilyOptions options = builder.build();
        assertThat(options.getKeys())
                .isEqualTo(new String[] { "enable_delta_files", "min_delta_size", "delta_file_size",
                        "delta_compression_type", "enable_delta_garbage_collection",
                        "delta_garbage_collection_age_cutoff", "delta_garbage_collection_force_threshold",
                        "delta_compaction_readahead_size", "delta_file_starting_level",
                        "prepopulate_delta_cache" });
        assertThat(options.getValues())
                .isEqualTo(new String[] { "false", "4096", "2048", "LZ4_COMPRESSION", "false", "0.91",
                        "0.96", "1024", "0", "PREPOPULATE_DELTA_DISABLE" });
    }

    /**
     * Configure the default column family with DELTAs.
     * Confirm that DELTAs are generated when appropriately-sized writes are
     * flushed.
     *
     * @throws RocksDBException if a db access throws an exception
     */
    @Test
    public void testDeltaWriteAboveThreshold() throws RocksDBException {
        try (final Options options = new Options()
                .setCreateIfMissing(true)
                .setMinDeltaSize(minDeltaSize)
                .setEnableDeltaFiles(true);

                final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
            db.put(small_key("default"), small_value("default"));
            db.flush(new FlushOptions().setWaitForFlush(true));

            // check there are no deltas in the database
            assertThat(countDBFiles(".sst")).isEqualTo(1);
            assertThat(countDBFiles(".delta")).isEqualTo(0);

            db.put(large_key("default"), large_value("default"));
            db.flush(new FlushOptions().setWaitForFlush(true));

            // wrote and flushed a value larger than the deltabing threshold
            // check there is a single delta in the database
            assertThat(countDBFiles(".sst")).isEqualTo(2);
            assertThat(countDBFiles(".delta")).isEqualTo(1);

            assertThat(db.get(small_key("default"))).isEqualTo(small_value("default"));
            assertThat(db.get(large_key("default"))).isEqualTo(large_value("default"));

            final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder fetchOptions = db.getOptions(null);
            assertThat(fetchOptions.minDeltaSize()).isEqualTo(minDeltaSize);
            assertThat(fetchOptions.enableDeltaFiles()).isEqualTo(true);
            assertThat(fetchOptions.writeBufferSize()).isEqualTo(64 << 20);
        }
    }

    /**
     * Configure 2 column families respectively with and without DELTAs.
     * Confirm that DELTA files are generated (once the DB is flushed) only for the
     * appropriate column
     * family.
     *
     * @throws RocksDBException if a db access throws an exception
     */
    @Test
    public void testDeltaWriteAboveThresholdCF() throws RocksDBException {
        final ColumnFamilyOptions columnFamilyOptions0 = new ColumnFamilyOptions();
        final ColumnFamilyDescriptor columnFamilyDescriptor0 = new ColumnFamilyDescriptor("default".getBytes(UTF_8),
                columnFamilyOptions0);
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections.singletonList(columnFamilyDescriptor0);
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true);
                final RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
                        columnFamilyDescriptors, columnFamilyHandles)) {
            db.put(columnFamilyHandles.get(0), small_key("default"), small_value("default"));
            db.flush(new FlushOptions().setWaitForFlush(true));

            assertThat(countDBFiles(".delta")).isEqualTo(0);

            try (final ColumnFamilyOptions columnFamilyOptions1 = new ColumnFamilyOptions()
                    .setMinDeltaSize(minDeltaSize).setEnableDeltaFiles(true);

                    final ColumnFamilyOptions columnFamilyOptions2 = new ColumnFamilyOptions()
                            .setMinDeltaSize(minDeltaSize).setEnableDeltaFiles(false)) {
                final ColumnFamilyDescriptor columnFamilyDescriptor1 = new ColumnFamilyDescriptor(
                        "column_family_1".getBytes(UTF_8), columnFamilyOptions1);
                final ColumnFamilyDescriptor columnFamilyDescriptor2 = new ColumnFamilyDescriptor(
                        "column_family_2".getBytes(UTF_8), columnFamilyOptions2);

                // Create the first column family with delta options
                db.createColumnFamily(columnFamilyDescriptor1);

                // Create the second column family with not-delta options
                db.createColumnFamily(columnFamilyDescriptor2);
            }
        }

        // Now re-open after auto-close - at this point the CF options we use are
        // recognized.
        try (final ColumnFamilyOptions columnFamilyOptions1 = new ColumnFamilyOptions().setMinDeltaSize(minDeltaSize)
                .setEnableDeltaFiles(true);

                final ColumnFamilyOptions columnFamilyOptions2 = new ColumnFamilyOptions().setMinDeltaSize(minDeltaSize)
                        .setEnableDeltaFiles(false)) {
            assertThat(columnFamilyOptions1.enableDeltaFiles()).isEqualTo(true);
            assertThat(columnFamilyOptions1.minDeltaSize()).isEqualTo(minDeltaSize);
            assertThat(columnFamilyOptions2.enableDeltaFiles()).isEqualTo(false);
            assertThat(columnFamilyOptions1.minDeltaSize()).isEqualTo(minDeltaSize);

            final ColumnFamilyDescriptor columnFamilyDescriptor1 = new ColumnFamilyDescriptor(
                    "column_family_1".getBytes(UTF_8), columnFamilyOptions1);
            final ColumnFamilyDescriptor columnFamilyDescriptor2 = new ColumnFamilyDescriptor(
                    "column_family_2".getBytes(UTF_8), columnFamilyOptions2);
            columnFamilyDescriptors = new ArrayList<>();
            columnFamilyDescriptors.add(columnFamilyDescriptor0);
            columnFamilyDescriptors.add(columnFamilyDescriptor1);
            columnFamilyDescriptors.add(columnFamilyDescriptor2);
            columnFamilyHandles = new ArrayList<>();

            assertThat(columnFamilyDescriptor1.getOptions().enableDeltaFiles()).isEqualTo(true);
            assertThat(columnFamilyDescriptor2.getOptions().enableDeltaFiles()).isEqualTo(false);

            try (final DBOptions dbOptions = new DBOptions();
                    final RocksDB db = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(),
                            columnFamilyDescriptors, columnFamilyHandles)) {
                final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder1 = db
                        .getOptions(columnFamilyHandles.get(1));
                assertThat(builder1.enableDeltaFiles()).isEqualTo(true);
                assertThat(builder1.minDeltaSize()).isEqualTo(minDeltaSize);

                final MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder builder2 = db
                        .getOptions(columnFamilyHandles.get(2));
                assertThat(builder2.enableDeltaFiles()).isEqualTo(false);
                assertThat(builder2.minDeltaSize()).isEqualTo(minDeltaSize);

                db.put(columnFamilyHandles.get(1), large_key("column_family_1_k2"),
                        large_value("column_family_1_k2"));
                db.flush(new FlushOptions().setWaitForFlush(true), columnFamilyHandles.get(1));
                assertThat(countDBFiles(".delta")).isEqualTo(1);

                db.put(columnFamilyHandles.get(2), large_key("column_family_2_k2"),
                        large_value("column_family_2_k2"));
                db.flush(new FlushOptions().setWaitForFlush(true), columnFamilyHandles.get(2));
                assertThat(countDBFiles(".delta")).isEqualTo(1);
            }
        }
    }
}
