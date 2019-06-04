package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * PutRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class PutRequest implements CoordinatingPayload {

    private byte[] key;
    private byte[] value;

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public int type() {
        return CoordinatingCommands.PUT_REQUEST.getType();
    }
}