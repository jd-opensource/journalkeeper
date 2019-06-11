package com.jd.journalkeeper.coordinating.state.domain;

import java.util.Arrays;

/**
 * StateWriteRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class StateWriteRequest extends StateRequest {

    private byte[] key;
    private byte[] expect;
    private byte[] value;

    public StateWriteRequest() {

    }

    public StateWriteRequest(int type, byte[] key) {
        super(type);
        this.key = key;
    }

    public StateWriteRequest(int type, byte[] key, byte[] value) {
        super(type);
        this.key = key;
        this.value = value;
    }

    public StateWriteRequest(int type, byte[] key, byte[] expect, byte[] value) {
        super(type);
        this.key = key;
        this.expect = expect;
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setExpect(byte[] expect) {
        this.expect = expect;
    }

    public byte[] getExpect() {
        return expect;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "StateWriteRequest{" +
                "type=" + getType() +
                "key=" + Arrays.toString(key) +
                ", expect=" + Arrays.toString(expect) +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}