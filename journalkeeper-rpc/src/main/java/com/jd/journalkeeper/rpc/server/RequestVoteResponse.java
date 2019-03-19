package com.jd.journalkeeper.rpc.server;

import com.jd.journalkeeper.rpc.BaseResponse;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class RequestVoteResponse  extends BaseResponse {

    private final int term;
    private final boolean voteGranted;

    public RequestVoteResponse(Throwable throwable) {
        this(throwable, -1, false);
    }


    public RequestVoteResponse(int term, boolean voteGranted) {
        this(null, term, voteGranted);
    }

    private RequestVoteResponse(Throwable throwable, int term, boolean voteGranted) {
        super(throwable);
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }
}
