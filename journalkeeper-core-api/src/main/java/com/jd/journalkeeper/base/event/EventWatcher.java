package com.jd.journalkeeper.base.event;


/**
 * @author liyue25
 * Date: 2019-03-14
 */
public interface EventWatcher {
    void onEvent(Event event);
}
