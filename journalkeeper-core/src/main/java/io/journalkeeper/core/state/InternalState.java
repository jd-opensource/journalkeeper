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

    public InternalState(ConfigState configState) {
        this.configState = configState;
        this.partitions = new HashSet<>(Collections.singleton(DEFAULT_PARTITION));
    }

    public InternalState(ConfigState configState, Set<Integer> partitions, URI preferredLeader) {
        this.configState = configState;
        this.partitions = partitions;
        this.preferredLeader = preferredLeader;
    }
    public URI getPreferredLeader() {
        return preferredLeader;
    }

    public void setPreferredLeader(URI preferredLeader) {
        this.preferredLeader = preferredLeader;
    }

    public Set<Integer> getPartitions() {
        return partitions;
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
}
