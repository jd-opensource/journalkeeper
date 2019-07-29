package com.jd.journalkeeper.coordinating.state.store;

import java.util.List;

/**
 * KVStore
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public interface KVStore {

    boolean set(byte[] key, byte[] value);

    byte[] get(byte[] key);

    List<byte[]> multiGet(List<byte[]> keys);

    boolean exist(byte[] key);

    boolean remove(byte[] key);

    boolean compareAndSet(byte[] key, byte[] expect, byte[] update);
}