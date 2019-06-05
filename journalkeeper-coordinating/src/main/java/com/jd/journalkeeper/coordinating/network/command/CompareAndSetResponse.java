package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * CompareAndSetResponse
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class CompareAndSetResponse implements CoordinatingPayload {

    private boolean success;
    private byte[] value;

    public CompareAndSetResponse() {

    }

    public CompareAndSetResponse(boolean success, byte[] value) {
        this.success = success;
        this.value = value;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public int type() {
        return CoordinatingCommands.COMPARE_AND_SET_RESPONSE.getType();
    }
}