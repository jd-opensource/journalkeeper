package io.journalkeeper.core.api;

/**
 * 读一致性模型。JournalKeeper支持3种读一致性：
 * STRICT：强一致，只在LEADER节点读取数据，保证强一致。
 * SEQUENTIAL：顺序一致，在所有节点读取数据，不保证每次读到的都是最新的数据，其它客户端写入的数据不一定会被马上读到，
 * 但可以保证每次读到的数据至少和上次读写的一样新，可以避免脏读。
 * NONE：不保证一致性。
 * @author LiYue
 * Date: 2020/2/19
 */
public enum QueryConsistency {
    STRICT(0),
    SEQUENTIAL(1),
    NONE(2);

    private int value;

    QueryConsistency(int value) {
        this.value = value;
    }

    public static QueryConsistency valueOf(final int value) {
        switch (value) {
            case 1:
                return SEQUENTIAL;
            case 2:
                return NONE;
            default:
                return STRICT;
        }
    }

    public int value() {
        return value;
    }

}
