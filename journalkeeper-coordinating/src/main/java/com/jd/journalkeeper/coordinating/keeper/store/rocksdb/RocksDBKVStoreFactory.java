package com.jd.journalkeeper.coordinating.keeper.store.rocksdb;

import com.jd.journalkeeper.coordinating.keeper.store.KVStore;
import com.jd.journalkeeper.coordinating.keeper.store.KVStoreFactory;

import java.nio.file.Path;
import java.util.Properties;

/**
 * RocksDBKVStoreFactory
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class RocksDBKVStoreFactory implements KVStoreFactory {

    @Override
    public KVStore create(Path path, Properties properties) {
        return new RocksDBKVStore(path, properties);
    }

    @Override
    public String type() {
        return "rocksdb";
    }
}