package com.jd.journalkeeper.rpc.server;

import java.net.URI;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class AsyncAppendEntriesRequest implements Termed{

    private final int term;
    private final URI leader;
    private final long prevLogIndex;
    private final int prevLogTerm;
    private final List<byte []> entries;
    private final long leaderCommit;

    public AsyncAppendEntriesRequest(int term, URI leader, long prevLogIndex, int prevLogTerm, List<byte []> entries, long leaderCommit) {
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

    public List<byte []> getEntries() {
        return entries;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }
}
