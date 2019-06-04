package com.jd.journalkeeper.coordinating.keeper.domain;

import java.io.Serializable;

/**
 * StateResponse
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class StateResponse implements Serializable {

    private int code;
    private String msg;
    private byte[] value;

    public StateResponse() {

    }

    public StateResponse(int code) {
        this.code = code;
    }

    public StateResponse(int code, byte[] value) {
        this.code = code;
        this.value = value;
    }

    public StateResponse(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public StateResponse(int code, String msg, byte[] value) {
        this.code = code;
        this.msg = msg;
        this.value = value;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }
}