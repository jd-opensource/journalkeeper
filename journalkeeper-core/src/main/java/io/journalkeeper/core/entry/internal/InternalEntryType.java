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
package io.journalkeeper.core.entry.internal;

public enum InternalEntryType {
    TYPE_LEADER_ANNOUNCEMENT(0),
    TYPE_CREATE_SNAPSHOT(1),
    TYPE_SCALE_PARTITIONS(2),
    TYPE_UPDATE_VOTERS_S1(3),
    TYPE_UPDATE_VOTERS_S2(4),
    TYPE_UPDATE_OBSERVERS(5),
    TYPE_SET_PREFERRED_LEADER(6),
    TYPE_RECOVER_SNAPSHOT(7),

    ;

    private int value;

    InternalEntryType(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static InternalEntryType valueOf(final int value) {
        switch (value) {
            case 0:
                return TYPE_LEADER_ANNOUNCEMENT;
            case 1:
                return TYPE_CREATE_SNAPSHOT;
            case 2:
                return TYPE_SCALE_PARTITIONS;
            case 3:
                return TYPE_UPDATE_VOTERS_S1;
            case 4:
                return TYPE_UPDATE_VOTERS_S2;
            case 5:
                return TYPE_UPDATE_OBSERVERS;
            case 6:
                return TYPE_SET_PREFERRED_LEADER;
            case 7:
                return TYPE_RECOVER_SNAPSHOT;
            default:
                throw new IllegalArgumentException("Illegal InternalEntryType value!");
        }
    }

}
