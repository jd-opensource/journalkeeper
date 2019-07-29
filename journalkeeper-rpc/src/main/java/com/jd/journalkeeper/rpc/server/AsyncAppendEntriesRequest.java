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
package com.jd.journalkeeper.rpc.server;

import java.net.URI;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class AsyncAppendEntriesRequest implements Termed{

    private final int term;
    private final URI leader;
    private final long prevLogIndex;
    private final int prevLogTerm;
    private final List<byte []> entries;
    private final long leaderCommit;

    public AsyncAppendEntriesRequest(int term, URI leader, long prevLogIndex, int prevLogTerm, List<byte []> entries, long leaderCommit) {
        this.term = term;
        this.leader = leader;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public int getTerm() {
        return term;
    }

    public URI getLeader() {
        return leader;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<byte []> getEntries() {
        return entries;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }
}
