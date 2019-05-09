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
