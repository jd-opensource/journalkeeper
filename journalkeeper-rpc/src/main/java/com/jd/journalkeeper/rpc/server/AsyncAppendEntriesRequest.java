package com.jd.journalkeeper.rpc.server;

import java.net.URI;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class AsyncAppendEntriesRequest<E> {

    private final int term;
    private final URI leader;
    private final long prevLogIndex;
    private final int prevLogTerm;
    private final List<E> entries;
    private final long leaderCommit;

    public AsyncAppendEntriesRequest(int term, URI leader, long prevLogIndex, int prevLogTerm, List<E> entries, long leaderCommit) {
        this.term = term;
        this.leader = leader;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public int getTerm() {
        return term;
    }

    public URI getLeader() {
        return leader;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<E> getEntries() {
        return entries;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }
}
