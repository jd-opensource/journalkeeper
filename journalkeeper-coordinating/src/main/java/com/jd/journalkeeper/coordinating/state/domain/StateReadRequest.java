package com.jd.journalkeeper.coordinating.state.domain;

/**
 * StateReadRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class StateReadRequest extends StateHeader {

    private byte[] key;

    public StateReadRequest() {

    }

    public StateReadRequest(int type, byte[] key) {
        super(type);
        this.key = key;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }
}