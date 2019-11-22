package io.journalkeeper.core.state;

import io.journalkeeper.core.journal.JournalSnapshot;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * JournalKeeper保留状态
 * @author LiYue
 * Date: 2019/11/20
 */
public class InternalState implements JournalSnapshot {
    private ConfigState configState;
    private URI preferredLeader = null;
    private Map<Integer /* partition */, Long /* lastIncludedIndex of the partition */> partitionIndices;
    private long lastIncludedIndex;
    private int lastIncludedTerm;
    private long minOffset;
    private long snapshotTimestamp = 0L;


    public InternalState() {}
    public InternalState(ConfigState configState, Set<Integer> partitions, URI preferredLeader) {
        this.configState = configState;
        this.partitionIndices = new HashMap<>(partitions.size());
        for (Integer partition : partitions) {
            partitionIndices.put(partition, 0L);
        }
        this.preferredLeader = preferredLeader;
        this.lastIncludedIndex = -1L;
        this.lastIncludedTerm = -1;
        this.minOffset = 0;

    }
    public URI getPreferredLeader() {
        return preferredLeader;
    }

    public void setPreferredLeader(URI preferredLeader) {
        this.preferredLeader = preferredLeader;
    }

    public Set<Integer> getPartitions() {
        return Collections.unmodifiableSet(partitionIndices.keySet());
    }

    public ConfigState getConfigState() {
        return configState;
    }

    public void setPartitions(Set<Integer> partitions) {
        Map<Integer, Long> copyOnWriteMap = new HashMap<>();
        for (Integer partition : partitions) {
            copyOnWriteMap.put(partition, partitionIndices.getOrDefault(partition, 0L));
        }
        partitionIndices = copyOnWriteMap;
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

    @Override
    public Map<Integer, Long> partitionMinIndices() {
        return Collections.unmodifiableMap(partitionIndices);
    }

    @Override
    public long minIndex() {
        return lastIncludedIndex + 1;
    }

    @Override
    public long minOffset() {
        return minOffset;
    }

    public void setPartitionIndices(Map<Integer, Long> partitionIndices) {
        this.partitionIndices = new HashMap<>(partitionIndices);
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getSnapshotTimestamp() {
        return snapshotTimestamp;
    }

    public void setSnapshotTimestamp(long snapshotTimestamp) {
        this.snapshotTimestamp = snapshotTimestamp;
    }

    public Map<Integer, Long> getPartitionIndices() {
        return Collections.unmodifiableMap(partitionIndices);
    }

    public long getMinOffset() {
        return minOffset;
    }
}
