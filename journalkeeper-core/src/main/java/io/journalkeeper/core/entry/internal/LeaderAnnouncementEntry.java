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
package io.journalkeeper.core.entry.internal;

import java.io.Serializable;
import java.net.URI;

import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_LEADER_ANNOUNCEMENT;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public class LeaderAnnouncementEntry extends InternalEntry implements Serializable {
    private static final long serialVersionUID = 2L;
    private final int term;
    private final URI leaderUri;

    public LeaderAnnouncementEntry(int term, URI leaderUri, int version) {
        super(TYPE_LEADER_ANNOUNCEMENT, version);
        this.term = term;
        this.leaderUri = leaderUri;
    }

    public LeaderAnnouncementEntry(int term, URI leaderUri) {
        super(TYPE_LEADER_ANNOUNCEMENT);
        this.term = term;
        this.leaderUri = leaderUri;
    }

    public int getTerm() {
        return term;
    }

    public URI getLeaderUri() {
        return leaderUri;
    }
}
