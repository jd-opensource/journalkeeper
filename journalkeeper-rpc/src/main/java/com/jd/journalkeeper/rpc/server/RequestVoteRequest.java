package com.jd.journalkeeper.rpc.server;

import java.net.URI;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class RequestVoteRequest {
    private final int term;
    private final URI candidate;
    private final long lastLogIndex;
    private final int lastLogTerm;

    public RequestVoteRequest(int term, URI candidate, long lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public URI getCandidate() {
        return candidate;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }
}
