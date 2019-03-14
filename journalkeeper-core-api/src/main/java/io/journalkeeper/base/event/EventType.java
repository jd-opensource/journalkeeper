package io.journalkeeper.base.event;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public class EventType {
    // 0 - 999 Journal Store API(JK-JS API)
    public static final int ON_JOURNAL_CHANGE = 0;

    // 1000 - 1999 Journal Keeper Configuration API（JK-C API）
    public static final int ON_LEADER_CHANGE = 1000;
    public static final int ON_VOTERS_CHANGE = 1001;
}
