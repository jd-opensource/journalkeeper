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


    public Set<Integer> getPartitions() {
        return partitions;
    }

    public void setPartitions(Set<Integer> partitions) {
        this.partitions = partitions;
    }

    @Override
    public int hashCode() {
        return Objects.hash(commitIndex, voters, parents, currentTerm, votedFor, thisServer, partitions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerMetadata that = (ServerMetadata) o;
        return commitIndex == that.commitIndex &&
                currentTerm == that.currentTerm &&
                Objects.equals(voters, that.voters) &&
                Objects.equals(parents, that.parents) &&
                Objects.equals(votedFor, that.votedFor) &&
                Objects.equals(thisServer, that.thisServer) &&
                Objects.equals(partitions, that.partitions);
    }
}
