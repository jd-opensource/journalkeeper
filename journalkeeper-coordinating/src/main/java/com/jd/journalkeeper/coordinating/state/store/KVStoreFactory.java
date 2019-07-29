package com.jd.journalkeeper.coordinating.state.store;

import java.nio.file.Path;
import java.util.Properties;

/**
 * KVStoreFactory
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public interface KVStoreFactory {

    KVStore create(Path path, Properties properties);

    String type();
}