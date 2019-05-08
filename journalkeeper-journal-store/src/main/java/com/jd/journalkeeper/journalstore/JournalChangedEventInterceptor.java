package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.utils.event.Event;
import com.jd.journalkeeper.utils.event.EventBus;
import com.jd.journalkeeper.utils.event.EventInterceptor;
import com.jd.journalkeeper.utils.event.EventType;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public class JournalChangedEventInterceptor implements EventInterceptor {
    @Override
    public boolean onEvent(Event event, EventBus eventBus) {
        if(event.getEventType() == EventType.ON_STATE_CHANGE) {
            eventBus.fireEvent(new Event(EventType.ON_JOURNAL_CHANGE, event.getEventData()));
        }
        return true;
    }
}
