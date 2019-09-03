package io.journalkeeper.rpc.client;

import io.journalkeeper.core.api.RaftServer;

/**
 * @author LiYue
 * Date: 2019-09-03
 */
public class ConvertRollRequest {
    private final RaftServer.Roll roll;

    public ConvertRollRequest(RaftServer.Roll roll) {
        this.roll = roll;
    }

    public RaftServer.Roll getRoll() {
        return roll;
    }
}
