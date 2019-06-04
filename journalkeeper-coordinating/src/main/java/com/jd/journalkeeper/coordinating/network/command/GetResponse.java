package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * GetResponse
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class GetResponse implements CoordinatingPayload {

    private byte[] value;
    private long createTime;
    private long modifyTime;

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return value;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(long modifyTime) {
        this.modifyTime = modifyTime;
    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_RESPONSE.getType();
    }
}