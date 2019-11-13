/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.api;

import java.util.Objects;

/**
 * 节点当前的状态
 * @author LiYue
 * Date: 2019-09-04
 */
public class ServerStatus {
    private RaftServer.Roll roll;
    private long minIndex;
    private long maxIndex;
    private long commitIndex;
    private long lastApplied;
    private VoterState voterState;

    public ServerStatus() {}

    public ServerStatus(RaftServer.Roll roll, long minIndex, long maxIndex, long commitIndex, long lastApplied, VoterState voterState) {
        this.roll = roll;
        this.minIndex = minIndex;
        this.maxIndex = maxIndex;
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
        this.voterState = voterState;
    }

    public RaftServer.Roll getRoll() {
        return roll;
    }

    public void setRoll(RaftServer.Roll roll) {
        this.roll = roll;
    }

    public long getMinIndex() {
        return minIndex;
    }

    public void setMinIndex(long minIndex) {
        this.minIndex = minIndex;
    }

    public long getMaxIndex() {
        return maxIndex;
    }

    public void setMaxIndex(long maxIndex) {
        this.maxIndex = maxIndex;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public VoterState getVoterState() {
        return voterState;
    }

    public void setVoterState(VoterState voterState) {
        this.voterState = voterState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerStatus that = (ServerStatus) o;
        return minIndex == that.minIndex &&
                maxIndex == that.maxIndex &&
                commitIndex == that.commitIndex &&
                lastApplied == that.lastApplied &&
                roll == that.roll &&
                voterState == that.voterState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(roll, minIndex, maxIndex, commitIndex, lastApplied, voterState);
    }
}
