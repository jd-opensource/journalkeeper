package com.jd.journalkeeper.coordinating.server.watcher;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Objects;

/**
 * WatcherKey
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/6
 */
public class WatcherKey {

    private byte[] key;

    public WatcherKey() {

    }

    public WatcherKey(byte[] key) {
        this.key = key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }

    @Override
    public int hashCode() {
        return ArrayUtils.hashCode(key);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof WatcherKey)) {
            return false;
        }
        return Objects.deepEquals(((WatcherKey) obj).getKey(), key);
    }
}