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
package io.journalkeeper.rpc.server;

import java.net.URI;
import java.util.Arrays;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InstallSnapshotRequest request = (InstallSnapshotRequest) o;
        return term == request.term &&
                lastIncludedIndex == request.lastIncludedIndex &&
                lastIncludedTerm == request.lastIncludedTerm &&
                offset == request.offset &&
                done == request.done &&
                leaderId.equals(request.leaderId) &&
                Arrays.equals(data, request.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, done);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
