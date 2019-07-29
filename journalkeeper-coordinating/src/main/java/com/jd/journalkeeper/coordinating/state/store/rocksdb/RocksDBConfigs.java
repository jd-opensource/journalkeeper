package com.jd.journalkeeper.coordinating.state.store.rocksdb;

/**
 * RocksDBConfigs
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class RocksDBConfigs {

    public static final String PREFIX = "rocksdb.";

    public static final String OPTIONS_PREFIX = PREFIX + "options.";

    public static final String TABLE_OPTIONS_PREFIX = PREFIX + "table.options.";

    public static final String FILTER_BITSPER_KEY = PREFIX + "filter.bitsPerKey";
}