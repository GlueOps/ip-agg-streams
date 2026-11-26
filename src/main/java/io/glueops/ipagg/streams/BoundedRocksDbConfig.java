package io.glueops.ipagg.streams;

import java.util.Map;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBufferManager;

/**
 * Caps RocksDB memory for the whole instance. A window store is split into segments, each its own RocksDB, per
 * partition; with the defaults (50 MiB block cache + 3 x 16 MiB memtables each) a few partitions take gigabytes off-heap.
 * Here all of them share one block cache, and memtables are charged against it.
 * Sizes: ROCKSDB_CACHE_MB (default 128), ROCKSDB_WRITE_BUFFER_MB (default 64, part of the cache).
 */
public class BoundedRocksDbConfig implements RocksDBConfigSetter {

    static {
        RocksDB.loadLibrary();
    }

    private static final long MIB = 1024 * 1024;
    private static final long CACHE_BYTES = envMb("ROCKSDB_CACHE_MB", 128) * MIB;
    private static final long WRITE_BUFFER_BYTES = envMb("ROCKSDB_WRITE_BUFFER_MB", 64) * MIB;
    private static final Cache CACHE = new LRUCache(CACHE_BYTES, -1, false, 0.1);
    private static final WriteBufferManager WRITE_BUFFERS = new WriteBufferManager(WRITE_BUFFER_BYTES, CACHE);

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        BlockBasedTableConfig table = (BlockBasedTableConfig) options.tableFormatConfig();
        table.setBlockCache(CACHE);
        table.setCacheIndexAndFilterBlocks(true);
        table.setCacheIndexAndFilterBlocksWithHighPriority(true);
        table.setPinTopLevelIndexAndFilter(true);
        options.setWriteBufferManager(WRITE_BUFFERS);
        options.setWriteBufferSize(8 * MIB);
        options.setMaxWriteBufferNumber(2);
        options.setTableFormatConfig(table);
    }

    @Override
    public void close(String storeName, Options options) {
        // The cache and write buffer manager are shared by every store: never close them here.
    }

    private static long envMb(String name, long fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : Long.parseLong(v.trim());
    }
}
