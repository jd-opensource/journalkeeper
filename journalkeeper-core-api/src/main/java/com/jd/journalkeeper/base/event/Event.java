package com.jd.journalkeeper.base.event;

import java.util.Map;

/**
 * @author liyue25
 * Date: 2019-04-12
 */
public class Event {
    private final int eventType;
    private final Map<String, Object> eventData;

    public Event(int eventType, Map<String, Object> eventData) {
        this.eventType = eventType;
        this.eventData = eventData;
    }

    public int getEventType() {
        return eventType;
    }

    public Map<String, Object> getEventData() {
        return eventData;
    }
}
