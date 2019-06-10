package com.jd.journalkeeper.coordinating.state.domain;

import java.io.Serializable;

/**
 * StateHeader
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class StateHeader implements Serializable {

    private int type;

    public StateHeader() {

    }

    public StateHeader(int type) {
        this.type = type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}