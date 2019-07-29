package com.jd.journalkeeper.coordinating.client;

import com.jd.journalkeeper.coordinating.state.domain.StateTypes;

/**
 * CoordinatingEvent
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/11
 */
public class CoordinatingEvent {

    private StateTypes type;
    private byte[] key;
    private byte[] value;

    public CoordinatingEvent() {

    }

    public CoordinatingEvent(StateTypes type, byte[] key) {
        this.type = type;
        this.key = key;
    }

    public CoordinatingEvent(StateTypes type, byte[] key, byte[] value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public StateTypes getType() {
        return type;
    }

    public void setType(StateTypes type) {
        this.type = type;
    }

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
}