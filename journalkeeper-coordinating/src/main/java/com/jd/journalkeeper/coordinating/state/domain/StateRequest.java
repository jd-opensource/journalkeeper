package com.jd.journalkeeper.coordinating.state.domain;

import java.io.Serializable;

/**
 * StateRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public abstract class StateRequest implements Serializable {

    private int type;

    public StateRequest() {

    }

    public StateRequest(int type) {
        this.type = type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}