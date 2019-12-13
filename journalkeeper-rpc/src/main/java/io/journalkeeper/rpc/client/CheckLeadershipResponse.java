package io.journalkeeper.rpc.client;

import io.journalkeeper.rpc.LeaderResponse;

/**
 * @author LiYue
 * Date: 2019/12/13
 */
public class CheckLeadershipResponse extends LeaderResponse {
    public CheckLeadershipResponse() {}
    public CheckLeadershipResponse(Throwable throwable) {
        super(throwable);
    }
}
