package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * ExistRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class ExistRequest implements CoordinatingPayload, KeyPayload {

    private byte[] key;

    public ExistRequest() {

    }

    public ExistRequest(byte[] key) {
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
        return CoordinatingCommands.EXIST_REQUEST.getType();
    }
}