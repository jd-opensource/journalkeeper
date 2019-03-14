package io.journalkeeper.base.event;

import java.util.Map;

/**
 * @author liyue25
 * Date: 2019-03-14
 */
public interface EventWatcher {
    void onEvent(int eventType, Map<String, String> eventData);
}
