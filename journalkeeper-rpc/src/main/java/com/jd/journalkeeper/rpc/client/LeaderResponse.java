package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.exceptions.NotLeaderException;
import com.jd.journalkeeper.rpc.BaseResponse;

import java.net.URI;

/**
 * @author liyue25
 * Date: 2019-03-20
 */
public class LeaderResponse extends BaseResponse {
    private final URI leader;

    protected LeaderResponse(){
        super(null);
        this.leader = null;
    }
    protected LeaderResponse(Throwable exception) {
        super(exception);
        leader = null;
    }

    protected LeaderResponse(URI leader) {
        super(new NotLeaderException());
        this.leader = leader;
    }

    public URI getLeader() {
        return leader;
    }
}
