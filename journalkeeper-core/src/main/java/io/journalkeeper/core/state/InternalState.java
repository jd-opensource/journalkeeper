package io.journalkeeper.core.state;

import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static io.journalkeeper.core.api.RaftJournal.DEFAULT_PARTITION;

/**
 * JournalKeeper保留状态
 * @author LiYue
 * Date: 2019/11/20
 */
public class InternalState {
    private ConfigState configState;
    private URI preferredLeader = null;
    private Set<Integer> partitions;
    private long lastIncludedIndex;
    private int lastIncludedTerm;


    public InternalState(ConfigState configState, Set<Integer> partitions, URI preferredLeader) {
        this.configState = configState;
        this.partitions = partitions;
        this.preferredLeader = preferredLeader;
        this.lastIncludedIndex = -1L;
        this.lastIncludedTerm = -1;
    }
    public URI getPreferredLeader() {
        return preferredLeader;
    }

    public void setPreferredLeader(URI preferredLeader) {
        this.preferredLeader = preferredLeader;
    }

    public Set<Integer> getPartitions() {
        return Collections.unmodifiableSet(partitions);
    }

    public ConfigState getConfigState() {
        return configState;
    }

    public void setPartitions(Set<Integer> partitions) {
        this.partitions = partitions;
    }

    public void setConfigState(ConfigState configState) {
        this.configState = configState;
    }

    public long getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public void setLastIncludedIndex(long lastIncludedIndex) {
        this.lastIncludedIndex = lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public void setLastIncludedTerm(int lastIncludedTerm) {
        this.lastIncludedTerm = lastIncludedTerm;
    }

    public void next() {
        lastIncludedIndex ++;
    }
}
