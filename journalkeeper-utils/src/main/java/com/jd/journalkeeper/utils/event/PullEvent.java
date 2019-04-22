package com.jd.journalkeeper.utils.event;


import java.util.Map;

/**
 * @author liyue25
 * Date: 2019-04-12
 */
public class PullEvent extends Event {
    private final long sequence;
    public PullEvent(int eventType, long sequence, Map<String, String> eventData) {
        super(eventType, eventData);
        this.sequence = sequence;
    }

    public long getSequence() {
        return sequence;
    }
}
