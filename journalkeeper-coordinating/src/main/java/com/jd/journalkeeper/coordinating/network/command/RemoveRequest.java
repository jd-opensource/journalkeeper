package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * RemoveRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class RemoveRequest implements CoordinatingPayload, KeyPayload {

    private byte[] key;

    public RemoveRequest() {

    }

    public RemoveRequest(byte[] key) {
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
        return CoordinatingCommands.REMOVE_REQUEST.getType();
    }
}