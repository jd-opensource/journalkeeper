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

import static io.journalkeeper.core.entry.internal.InternalEntryType.TYPE_SET_PREFERRED_LEADER;

/**
 * @author LiYue
 * Date: 2019-09-11
 */
public class SetPreferredLeaderEntry extends InternalEntry implements Serializable {
    private static final long serialVersionUID = 1L;
    private final URI preferredLeader;

    public SetPreferredLeaderEntry(URI preferredLeader) {
        super(TYPE_SET_PREFERRED_LEADER);
        this.preferredLeader = preferredLeader;
    }

    public SetPreferredLeaderEntry(URI preferredLeader, int version) {
        super(TYPE_SET_PREFERRED_LEADER, version);
        this.preferredLeader = preferredLeader;
    }

    public URI getPreferredLeader() {
        return preferredLeader;
    }
}
