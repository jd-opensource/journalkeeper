package com.jd.journalkeeper.persistence;

import java.net.URI;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-03-20
 */
public class ServerMetadata {
    private long commitIndex = 0L;
    private List<URI> voters;
    private List<URI> parents;
    private int currentTerm = 0;
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

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
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

    @Override
    public boolean equals(Object obj) {
// checking if both the object references are
        // referring to the same object.
        if(this == obj)
            return true;

        // it checks if the argument is of the
        // type Geek by comparing the classes
        // of the passed argument and this object.
        // if(!(obj instanceof Geek)) return false; ---> avoid.
        if(obj == null || obj.getClass()!= this.getClass())
            return false;

        // type casting of the argument.
        ServerMetadata serverMetadata = (ServerMetadata) obj;

        // comparing the state of argument with
        // the state of 'this' Object.
        return (serverMetadata.commitIndex == this.commitIndex &&
                serverMetadata.currentTerm == this.currentTerm &&
                objectEquals(serverMetadata.thisServer, this.thisServer) &&
                objectEquals(serverMetadata.votedFor, this.votedFor) &&
                objectEquals(serverMetadata.voters, this.voters) &&
                objectEquals(serverMetadata.parents, this.parents)
                );
    }

    private static boolean objectEquals(Object o1, Object o2) {
        if(o1 == null && o2 == null) return true;
        if(o1 != null && o2 != null) {
            return o1.equals(o2);
        } else {
            return false;
        }
    }
}
