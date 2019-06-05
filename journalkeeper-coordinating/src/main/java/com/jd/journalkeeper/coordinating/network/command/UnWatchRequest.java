package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * WatchRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class UnWatchRequest implements CoordinatingPayload, KeyPayload {

    private byte[] key;

    public UnWatchRequest() {

    }

    public UnWatchRequest(byte[] key) {
        this.key = key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public int type() {
        return CoordinatingCommands.UN_WATCH_REQUEST.getType();
    }
}