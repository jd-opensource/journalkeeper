package io.journalkeeper.core.state;

import java.net.URI;
import java.util.List;
import java.util.Set;

/**
 * @author LiYue
 * Date: 2019/11/21
 */
public class PersistInternalState {
    private URI preferredLeader = null;
    private Set<Integer> partitions;
    private long lastIncludedIndex;
    private int lastIncludedTerm;
    private List<URI> configNew ;
    private List<URI> configOld ;
    private boolean jointConsensus;


    public URI getPreferredLeader() {
        return preferredLeader;
    }

    public void setPreferredLeader(URI preferredLeader) {
        this.preferredLeader = preferredLeader;
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    public void setPartitions(Set<Integer> partitions) {
        this.partitions = partitions;
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

    public List<URI> getConfigNew() {
        return configNew;
    }

    public void setConfigNew(List<URI> configNew) {
        this.configNew = configNew;
    }

    public List<URI> getConfigOld() {
        return configOld;
    }

    public void setConfigOld(List<URI> configOld) {
        this.configOld = configOld;
    }

    public boolean isJointConsensus() {
        return jointConsensus;
    }

    public void setJointConsensus(boolean jointConsensus) {
        this.jointConsensus = jointConsensus;
    }

    InternalState toInternalState() {
        ConfigState configState;
        if(isJointConsensus()) {
            configState = new ConfigState(configOld, configNew);
        } else {
            configState = new ConfigState(configNew);
        }
        InternalState internalState = new InternalState(configState, partitions, preferredLeader);
        internalState.setLastIncludedTerm(getLastIncludedTerm());
        internalState.setLastIncludedIndex(getLastIncludedIndex());
        return internalState;
    }

    PersistInternalState fromInternalState(InternalState internalState) {
        ConfigState configState = internalState.getConfigState();
        setJointConsensus(configState.isJointConsensus());
        setConfigNew(configState.getConfigNew());
        setConfigOld(configState.getConfigOld());

        setLastIncludedIndex(internalState.getLastIncludedIndex());
        setLastIncludedTerm(internalState.getLastIncludedTerm());
        setPartitions(internalState.getPartitions());
        setPreferredLeader(internalState.getPreferredLeader());
        return this;
    }
}
