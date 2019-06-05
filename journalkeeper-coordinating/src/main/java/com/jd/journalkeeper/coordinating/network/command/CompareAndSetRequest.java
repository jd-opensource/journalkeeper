package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * CompareAndSetRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class CompareAndSetRequest implements CoordinatingPayload, KeyPayload {

    private byte[] key;
    private byte[] expect;
    private byte[] update;

    public CompareAndSetRequest() {

    }

    public CompareAndSetRequest(byte[] key, byte[] expect, byte[] update) {
        this.key = key;
        this.expect = expect;
        this.update = update;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getExpect() {
        return expect;
    }

    public void setExpect(byte[] expect) {
        this.expect = expect;
    }

    public byte[] getUpdate() {
        return update;
    }

    public void setUpdate(byte[] update) {
        this.update = update;
    }

    @Override
    public int type() {
        return CoordinatingCommands.COMPARE_AND_SET_REQUEST.getType();
    }
}