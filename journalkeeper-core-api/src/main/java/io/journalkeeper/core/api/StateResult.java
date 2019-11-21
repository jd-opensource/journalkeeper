package io.journalkeeper.core.api;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public class StateResult<ER> {
    private final ER userResult;
    private final Map<String, String> eventData;
    private long lastApplied;
    public StateResult(ER userResult) {
        this(userResult, new HashMap<>());
    }
    public StateResult(ER userResult, Map<String, String> eventData) {
        this.userResult = userResult;
        this.eventData = eventData == null ? new HashMap<>() : eventData;
    }

    public ER getUserResult() {
        return userResult;
    }

    public Map<String, String> getEventData() {
        return eventData;
    }

    public String putEventData(String key, String value) {
        return eventData.put(key, value);
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }
}
