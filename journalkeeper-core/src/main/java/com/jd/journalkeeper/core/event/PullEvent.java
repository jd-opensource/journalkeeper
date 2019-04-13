package com.jd.journalkeeper.core.event;

import com.jd.journalkeeper.base.event.Event;

import java.util.Map;

/**
 * @author liyue25
 * Date: 2019-04-12
 */
public class PullEvent extends Event {
    private final long sequence;
    public PullEvent(int eventType, Map<String, String> eventData, long sequence) {
        super(eventType, eventData);
        this.sequence = sequence;
    }

    public long getSequence() {
        return sequence;
    }
}
