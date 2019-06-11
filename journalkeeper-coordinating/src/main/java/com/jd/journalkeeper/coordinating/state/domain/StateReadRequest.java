package com.jd.journalkeeper.coordinating.state.domain;

import java.util.Arrays;
import java.util.List;

/**
 * StateReadRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class StateReadRequest extends StateRequest {

    private byte[] key;
    private List<byte[]> keys;

    public StateReadRequest() {

    }

    public StateReadRequest(int type, byte[] key) {
        super(type);
        this.key = key;
    }

    public StateReadRequest(int type, List<byte[]> keys) {
        super(type);
        this.keys = keys;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setKeys(List<byte[]> keys) {
        this.keys = keys;
    }

    public List<byte[]> getKeys() {
        return keys;
    }

    @Override
    public String toString() {
        return "StateReadRequest{" +
                "type=" + getType() +
                "key=" + Arrays.toString(key) +
                ", keys=" + keys +
                '}';
    }
}