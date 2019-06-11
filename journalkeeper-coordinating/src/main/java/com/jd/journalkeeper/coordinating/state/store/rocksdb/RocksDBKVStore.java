package com.jd.journalkeeper.coordinating.state.store.rocksdb;

import com.jd.journalkeeper.coordinating.state.exception.CoordinatingStateException;
import com.jd.journalkeeper.coordinating.state.store.KVStore;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * RocksDBKVStore
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class RocksDBKVStore implements KVStore {

    private static final StringBuilder STRING_BUILDER_CACHE = new StringBuilder();

    protected static final Logger logger = LoggerFactory.getLogger(RocksDBKVStore.class);

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
            Options options = parseOptions(properties);
            return RocksDB.open(options, path.toString());
        } catch (Exception e) {
            throw new CoordinatingStateException(e);
        }
    }

    protected Options parseOptions(Properties properties) {
        return RocksDBConfigParser.parse(properties);
    }

    @Override
    public boolean set(byte[] key, byte[] value) {
        try {
            rocksDB.put(key, value);
            return true;
        } catch (RocksDBException e) {
            throw new CoordinatingStateException(e);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        try {
            return rocksDB.get(key);
        } catch (RocksDBException e) {
            throw new CoordinatingStateException(e);
        }
    }

    @Override
    public List<byte[]> multiGet(List<byte[]> keys) {
        try {
            return rocksDB.multiGetAsList(keys);
        } catch (RocksDBException e) {
            throw new CoordinatingStateException(e);
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
            throw new CoordinatingStateException(e);
        }
    }

    @Override
    public boolean compareAndSet(byte[] key, byte[] expect, byte[] update) {
        try {
            byte[] current = rocksDB.get(key);
            if (current != null && !Objects.deepEquals(current, expect)) {
                return false;
            }
            rocksDB.put(key, update);
            return true;
        } catch (RocksDBException e) {
            throw new CoordinatingStateException(e);
        }
    }
}