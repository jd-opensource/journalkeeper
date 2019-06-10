package com.jd.journalkeeper.coordinating.state.store;

/**
 * KVStore
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public interface KVStore {

    boolean put(byte[] key, byte[] value);

    byte[] get(byte[] key);

    boolean exist(byte[] key);

    boolean remove(byte[] key);

    boolean compareAndSet(byte[] key, byte[] expect, byte[] update);
}