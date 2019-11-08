package io.journalkeeper.core.entry.reserved;

public enum ReservedEntryType {
    TYPE_LEADER_ANNOUNCEMENT(0),
    TYPE_COMPACT_JOURNAL(1),
    TYPE_SCALE_PARTITIONS(2),
    TYPE_UPDATE_VOTERS_S1(3),
    TYPE_UPDATE_VOTERS_S2(4),
    TYPE_UPDATE_OBSERVERS(5),
    TYPE_SET_PREFERRED_LEADER(6);
    private int value;

    ReservedEntryType(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static ReservedEntryType valueOf(final int value) {
        switch (value) {
            case 0:
                return TYPE_LEADER_ANNOUNCEMENT;
            case 1:
                return TYPE_COMPACT_JOURNAL;
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
            default:
                throw new IllegalArgumentException("Illegal ReservedEntryType value!");
        }
    }

}
