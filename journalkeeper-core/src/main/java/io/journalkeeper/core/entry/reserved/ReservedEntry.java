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
package io.journalkeeper.core.entry.reserved;

/**
 * @author LiYue
 * Date: 2019-05-09
 */
public abstract class ReservedEntry {
    public final static int TYPE_LEADER_ANNOUNCEMENT = 0;
    public final static int TYPE_COMPACT_JOURNAL = 1;
    public final static int TYPE_SCALE_PARTITIONS = 2;
    public final static int TYPE_UPDATE_VOTERS_S1 = 3;
    public final static int TYPE_UPDATE_VOTERS_S2 = 4;
    public final static int TYPE_UPDATE_OBSERVERS = 5;
    private final int type;

    protected ReservedEntry(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
