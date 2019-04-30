package com.jd.journalkeeper.rpc;

import com.jd.journalkeeper.exceptions.NotLeaderException;

import java.net.URI;

/**
 * 请求LEADER方法的通用Response。
 * 当请求的节点不是LEADER时：
 * statusCode == StatusCode.NOT_LEADER
 * getLeader() 返回当前LEADER的URI。
 *
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
            setStatusCode(StatusCode.NOT_LEADER);
            setLeader(e.getLeader());
        } catch (Throwable t) {
            super.setException(throwable);
        }
    }

    /**
     * 当前LEADER的URI
     * @return 当前LEADER的URI
     */
    public URI getLeader() {
        return leader;
    }

    public void setLeader(URI leader) {
        this.leader = leader;
    }
}