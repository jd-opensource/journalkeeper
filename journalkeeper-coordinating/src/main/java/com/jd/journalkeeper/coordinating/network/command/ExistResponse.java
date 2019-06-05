package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * ExistResponse
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class ExistResponse implements CoordinatingPayload {

    private boolean exist;

    public void setExist(boolean exist) {
        this.exist = exist;
    }

    public boolean isExist() {
        return exist;
    }

    @Override
    public int type() {
        return CoordinatingCommands.EXIST_RESPONSE.getType();
    }
}