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
package io.journalkeeper.persistence;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author LiYue
 * Date: 2019-03-20
 */
public class ServerMetadata {
    private long commitIndex = 0L;
    private List<URI> voters;
    private List<URI> parents;
    private int currentTerm = 0;
    private URI votedFor;
    private URI thisServer;
    private Set<Integer> partitions;

    private List<URI> oldVoters;
    private boolean jointConsensus = false;
    private URI preferredLeader;


    public long getCommitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public List<URI> getVoters() {
        return voters;
    }

    public synchronized void setVoters(List<URI> voters) {
        this.voters = voters;
    }

    public List<URI> getParents() {
        return parents;
    }

    public synchronized void setParents(List<URI> parents) {
        this.parents = parents;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public synchronized void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public URI getVotedFor() {
        return votedFor;
    }

    public synchronized void setVotedFor(URI votedFor) {
        this.votedFor = votedFor;
    }

    public URI getThisServer() {
        return thisServer;
    }

    public synchronized void setThisServer(URI thisServer) {
        this.thisServer = thisServer;
    }


    public Set<Integer> getPartitions() {
        return partitions;
    }

    public synchronized void setPartitions(Set<Integer> partitions) {
        this.partitions = partitions;
    }

    public List<URI> getOldVoters() {
        return oldVoters;
    }

    public synchronized void setOldVoters(List<URI> oldVoters) {
        this.oldVoters = oldVoters;
    }

    public boolean isJointConsensus() {
        return jointConsensus;
    }

    public synchronized void setJointConsensus(boolean jointConsensus) {
        this.jointConsensus = jointConsensus;
    }

    public URI getPreferredLeader() {
        return preferredLeader;
    }

    public void setPreferredLeader(URI preferredLeader) {
        this.preferredLeader = preferredLeader;
    }

    @Override
    public synchronized ServerMetadata clone() {
        ServerMetadata clone = new ServerMetadata();
        clone.commitIndex = this.commitIndex;
        clone.voters = new ArrayList<>(this.voters);
        clone.parents = new ArrayList<>(this.parents);
        clone.currentTerm = this.currentTerm;
        clone.votedFor = this.votedFor;
        clone.thisServer = this.thisServer;
        clone.partitions = new HashSet<>(this.partitions);
        clone.oldVoters = new ArrayList<>(this.oldVoters);
        clone.jointConsensus = this.jointConsensus;
        clone.preferredLeader = this.preferredLeader;
        return clone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerMetadata that = (ServerMetadata) o;
        return commitIndex == that.commitIndex &&
                currentTerm == that.currentTerm &&
                jointConsensus == that.jointConsensus &&
                Objects.equals(voters, that.voters) &&
                Objects.equals(parents, that.parents) &&
                Objects.equals(votedFor, that.votedFor) &&
                Objects.equals(thisServer, that.thisServer) &&
                Objects.equals(partitions, that.partitions) &&
                Objects.equals(oldVoters, that.oldVoters) &&
                Objects.equals(preferredLeader, that.preferredLeader);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commitIndex, voters, parents, currentTerm, votedFor, thisServer, partitions, oldVoters, jointConsensus, preferredLeader);
    }


}
