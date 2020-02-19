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
package io.journalkeeper.persistence;

import java.net.URI;
import java.util.List;
import java.util.Objects;

/**
 * @author LiYue
 * Date: 2019-03-20
 */
public class ServerMetadata {
    private long commitIndex = 0L;
    private List<URI> parents;
    private int currentTerm = 0;
    private URI votedFor;
    private URI thisServer;
    private boolean initialized = false;

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
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

    public boolean isInitialized() {
        return initialized;
    }

    public void setInitialized(boolean initialized) {
        this.initialized = initialized;
    }

    @Override
    public String toString() {
        return "ServerMetadata{" +
                "commitIndex=" + commitIndex +
                ", parents=" + parents +
                ", currentTerm=" + currentTerm +
                ", votedFor=" + votedFor +
                ", thisServer=" + thisServer +
                ", initialized=" + initialized +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerMetadata metadata = (ServerMetadata) o;
        return commitIndex == metadata.commitIndex &&
                currentTerm == metadata.currentTerm &&
                initialized == metadata.initialized &&
                Objects.equals(parents, metadata.parents) &&
                Objects.equals(votedFor, metadata.votedFor) &&
                Objects.equals(thisServer, metadata.thisServer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commitIndex, parents, currentTerm, votedFor, thisServer, initialized);
    }
}
