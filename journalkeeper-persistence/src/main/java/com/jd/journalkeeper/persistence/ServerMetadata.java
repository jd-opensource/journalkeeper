package com.jd.journalkeeper.persistence;

import java.net.URI;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-03-20
 */
public class ServerMetadata {
    private long commitIndex;
    private List<URI> voters;
    private List<URI> parents;
    private Integer currentTerm;
    private URI votedFor;
    private URI thisServer;

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public List<URI> getVoters() {
        return voters;
    }

    public void setVoters(List<URI> voters) {
        this.voters = voters;
    }

    public List<URI> getParents() {
        return parents;
    }

    public void setParents(List<URI> parents) {
        this.parents = parents;
    }

    public Integer getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(Integer currentTerm) {
        this.currentTerm = currentTerm;
    }

    public URI getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(URI votedFor) {
        this.votedFor = votedFor;
    }

    public URI getThisServer() {
        return thisServer;
    }

    public void setThisServer(URI thisServer) {
        this.thisServer = thisServer;
    }
}
