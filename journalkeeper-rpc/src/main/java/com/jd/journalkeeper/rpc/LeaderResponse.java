package com.jd.journalkeeper.rpc;

import com.jd.journalkeeper.exceptions.NotLeaderException;

import java.net.URI;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public abstract class LeaderResponse extends BaseResponse {
    protected URI leader;

    public LeaderResponse () {}
    public LeaderResponse(Throwable throwable) {
        super(throwable);
    }

    public LeaderResponse(StatusCode statusCode) {
        super(statusCode);
    }

    @Override
    public void setException(Throwable throwable) {
        try {
            throw throwable;
        } catch (NotLeaderException e) {
            setLeader(e.getLeader());
        } catch (Throwable t) {
            super.setException(throwable);
        }

    }

    public URI getLeader() {
        return leader;
    }

    public void setLeader(URI leader) {
        this.leader = leader;
    }
}
