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
package com.jd.journalkeeper.core.entry.reserved;

import com.jd.journalkeeper.base.Serializer;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class LeaderAnnouncementEntrySerializer implements Serializer<LeaderAnnouncementEntry> {
    @Override
    public int sizeOf(LeaderAnnouncementEntry leaderAnnouncementEntry) {
        return Byte.BYTES;
    }

    @Override
    public byte[] serialize(LeaderAnnouncementEntry entry) {
        return new byte [] {LeaderAnnouncementEntry.TYPE_LEADER_ANNOUNCEMENT};
    }

    @Override
    public LeaderAnnouncementEntry parse(byte[] bytes) {
        return new LeaderAnnouncementEntry();
    }
}
