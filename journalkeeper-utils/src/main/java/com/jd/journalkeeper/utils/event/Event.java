package com.jd.journalkeeper.utils.event;

import java.util.Map;

/**
 * @author liyue25
 * Date: 2019-04-12
 */
public class Event {
    private final int eventType;
    private final Map<String, String> eventData;

    public Event(int eventType, Map<String, String> eventData) {
        this.eventType = eventType;
        this.eventData = eventData;
    }

    public int getEventType() {
        return eventType;
    }

    public Map<String, String> getEventData() {
        return eventData;
    }
}
