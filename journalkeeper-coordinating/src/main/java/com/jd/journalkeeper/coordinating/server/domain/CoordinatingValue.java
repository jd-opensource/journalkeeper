package com.jd.journalkeeper.coordinating.server.domain;

import java.io.Serializable;

/**
 * CoordinatingValue
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class CoordinatingValue implements Serializable {

    private byte[] value;
    private long modifyTime;
    private long createTime;

    public CoordinatingValue() {

    }

    public CoordinatingValue(byte[] value, long modifyTime, long createTime) {
        this.value = value;
        this.modifyTime = modifyTime;
        this.createTime = createTime;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public long getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(long modifyTime) {
        this.modifyTime = modifyTime;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
}