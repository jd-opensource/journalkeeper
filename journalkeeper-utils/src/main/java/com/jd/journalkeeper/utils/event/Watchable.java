package com.jd.journalkeeper.utils.event;

/**
 * @author liyue25
 * Date: 2019-04-23
 */
public interface Watchable {
    void watch(EventWatcher eventWatcher);

    void unWatch(EventWatcher eventWatcher);
}
