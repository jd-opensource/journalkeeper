package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * WatchRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class WatchRequest implements CoordinatingPayload {

    private byte[] key;
    private boolean once;

    public WatchRequest() {

    }

    public WatchRequest(byte[] key, boolean once) {
        this.key = key;
        this.once = once;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }

    public void setOnce(boolean once) {
        this.once = once;
    }

    public boolean isOnce() {
        return once;
    }

    @Override
    public int type() {
        return CoordinatingCommands.WATCH_REQUEST.getType();
    }
}