/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.state;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019/11/21
 */
public class PersistInternalState {
    private URI preferredLeader = null;
    private Map<Integer /* partition */, Long /* lastIncludedIndex of the partition */> partitionIndices;
    private long lastIncludedIndex;
    private int lastIncludedTerm;
    private List<URI> configNew;
    private List<URI> configOld;
    private boolean jointConsensus;
    private long minOffset;
    private long snapshotTimestamp;


    public URI getPreferredLeader() {
        return preferredLeader;
    }

    public void setPreferredLeader(URI preferredLeader) {
        this.preferredLeader = preferredLeader;
    }

    public Map<Integer, Long> getPartitionIndices() {
        return partitionIndices;
    }

    public void setPartitionIndices(Map<Integer, Long> partitionIndices) {
        this.partitionIndices = partitionIndices;
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
        if (isJointConsensus()) {
            configState = new ConfigState(configOld, configNew);
        } else {
            configState = new ConfigState(configNew);
        }
        InternalState internalState = new InternalState();
        internalState.setConfigState(configState);
        internalState.setPartitionIndices(getPartitionIndices());
        internalState.setPreferredLeader(getPreferredLeader());
        internalState.setLastIncludedTerm(getLastIncludedTerm());
        internalState.setLastIncludedIndex(getLastIncludedIndex());
        internalState.setMinOffset(getMinOffset());
        internalState.setSnapshotTimestamp(getSnapshotTimestamp());
        return internalState;
    }

    PersistInternalState fromInternalState(InternalState internalState) {
        ConfigState configState = internalState.getConfigState();
        setJointConsensus(configState.isJointConsensus());
        setConfigNew(configState.getConfigNew());
        setConfigOld(configState.getConfigOld());

        setLastIncludedIndex(internalState.getLastIncludedIndex());
        setLastIncludedTerm(internalState.getLastIncludedTerm());
        setPartitionIndices(internalState.getPartitionIndices());
        setPreferredLeader(internalState.getPreferredLeader());
        setMinOffset(internalState.getMinOffset());
        setSnapshotTimestamp(internalState.getSnapshotTimestamp());
        return this;
    }

    public long getMinOffset() {
        return minOffset;
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

    @Override
    public String toString() {
        return "PersistInternalState{" +
                "preferredLeader=" + preferredLeader +
                ", partitionIndices=" + partitionIndices +
                ", lastIncludedIndex=" + lastIncludedIndex +
                ", lastIncludedTerm=" + lastIncludedTerm +
                ", configNew=" + configNew +
                ", configOld=" + configOld +
                ", jointConsensus=" + jointConsensus +
                ", minOffset=" + minOffset +
                ", snapshotTimestamp=" + snapshotTimestamp +
                '}';
    }
}
