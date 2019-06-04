package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

/**
 * GetClusterRequest
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class GetClusterRequest implements CoordinatingPayload {

    @Override
    public int type() {
        return CoordinatingCommands.GET_CLUSTER_REQUEST.getType();
    }
}