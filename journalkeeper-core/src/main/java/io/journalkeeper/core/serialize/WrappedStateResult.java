package io.journalkeeper.core.serialize;

import java.util.Map;

/**
 * @author LiYue
 * Date: 2020/2/18
 */
public class WrappedStateResult<ER> {
    private final ER result;
    private final Map<String, String> eventData;

    public WrappedStateResult(ER result, Map<String, String> eventData) {
        this.result = result;
        this.eventData = eventData;
    }

    public ER getResult() {
        return result;
    }

    public Map<String, String> getEventData() {
        return eventData;
    }
}
