package io.journalkeeper.rpc.server;

import java.net.URI;

/**
 * InstallSnapshot RPC
 * Invoked by leader to send chunks of a snapshot to a follower. Leaders always send chunks in order.
 *
 * @author LiYue
 * Date: 2019/11/21
 */
public class InstallSnapshotRequest {
    // leader’s term
    private final int term;
    // so follower can redirect clients
    private final URI leaderId;
    // the snapshot replaces all entries up through and including this index
    private final long lastIncludedIndex;
    // term of lastIncludedIndex
    private final int lastIncludedTerm;
    // byte offset where chunk is positioned in the snapshot file
    private final int offset;
    // raw bytes of the snapshot chunk, starting at offset
    private final byte [] data;
    // true if this is the last chunk
    private final boolean done;

    public InstallSnapshotRequest(int term, URI leaderId, long lastIncludedIndex, int lastIncludedTerm, int offset, byte[] data, boolean done) {
        this.term = term;
        this.leaderId = leaderId;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.offset = offset;
        this.data = data;
        this.done = done;
    }

    public int getTerm() {
        return term;
    }

    public URI getLeaderId() {
        return leaderId;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isDone() {
        return done;
    }

    @Override
    public String toString() {
        return "InstallSnapshotRequest{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", lastIncludedIndex=" + lastIncludedIndex +
                ", lastIncludedTerm=" + lastIncludedTerm +
                ", offset=" + offset +
                ", done=" + done +
                '}';
    }
}
