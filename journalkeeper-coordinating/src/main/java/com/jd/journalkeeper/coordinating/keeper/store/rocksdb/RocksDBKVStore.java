package com.jd.journalkeeper.coordinating.keeper.store.rocksdb;

import com.jd.journalkeeper.coordinating.keeper.store.KVStore;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.util.SizeUnit;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

/**
 * RocksDBKVStore
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
// TODO 异常处理
public class RocksDBKVStore implements KVStore {

    private static final StringBuilder STRING_BUILDER_CACHE = new StringBuilder();

    private Path path;
    private Properties properties;
    private RocksDB rocksDB;

    static {
        RocksDB.loadLibrary();
    }

    public RocksDBKVStore(Path path, Properties properties) {
        this.path = path;
        this.properties = properties;
        this.rocksDB = init(path, properties);
    }

    protected RocksDB init(Path path, Properties properties) {
        try {
            Options options = convertOptions(properties);
            return RocksDB.open(options, path.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Options convertOptions(Properties properties) {
        // TODO 配置转换
        Options options = new Options();
        options.setCreateIfMissing(true);

        options.setCreateIfMissing(true)
                .setWriteBufferSize(128 * SizeUnit.MB)
                .setMinWriteBufferNumberToMerge(2)
                .setLevel0FileNumCompactionTrigger(10)

                .setTargetFileSizeBase(256 * SizeUnit.MB)
                .setMaxBytesForLevelBase(256 * SizeUnit.MB * 10)
                .setTargetFileSizeMultiplier(10)

                .setMaxBackgroundCompactions(Runtime.getRuntime().availableProcessors())
                .setMaxBackgroundFlushes(1)

                .setAllowConcurrentMemtableWrite(false)
                .setAllowMmapWrites(true)
                .setSkipStatsUpdateOnDbOpen(true)
                .setOptimizeFiltersForHits(true)
//                .setLevelCompactionDynamicLevelBytes(true)
                .setNewTableReaderForCompactionInputs(true)
                .setCompressionPerLevel(Arrays.asList(CompressionType.NO_COMPRESSION, CompressionType.NO_COMPRESSION, CompressionType.LZ4_COMPRESSION, CompressionType.ZLIB_COMPRESSION))
//                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setCompactionStyle(CompactionStyle.LEVEL);

        LRUCache blockCache = new LRUCache(1024 * SizeUnit.MB, -1);
        BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setBlockCache(blockCache)
                .setBlockSize(256 * SizeUnit.KB)
                .setCacheIndexAndFilterBlocks(true)
//                .setFilterPolicy(bloomFilter)
                .setBlockCacheCompressed(blockCache);

        options.setMemTableConfig(new SkipListMemTableConfig());
        options.setTableFormatConfig(tableOptions);
        return options;
    }

    @Override
    public boolean put(byte[] key, byte[] value) {
        try {
            rocksDB.put(key, value);
            return true;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return rocksDB.get(key);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean exist(byte[] key) {
        return rocksDB.keyMayExist(key, STRING_BUILDER_CACHE);
    }

    @Override
    public boolean remove(byte[] key) {
        try {
            if (!rocksDB.keyMayExist(key, STRING_BUILDER_CACHE)) {
                return false;
            }
            rocksDB.delete(key);
            return true;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean compareAndSet(byte[] key, byte[] expect, byte[] update) {
        try {
            byte[] current = rocksDB.get(key);
            if (current == null || Objects.deepEquals(current, expect)) {
                rocksDB.put(key, update);
                return true;
            } else {
                return false;
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }
}