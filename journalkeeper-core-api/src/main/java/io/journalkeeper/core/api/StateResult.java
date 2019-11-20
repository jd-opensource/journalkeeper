package io.journalkeeper.core.api;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author LiYue
 * Date: 2019/11/20
 */
public class StateResult<ER> {
    private final ER userResult;
    private final Map<String, String> eventData;
    public StateResult(ER userResult) {
        this(userResult, Collections.emptyMap());
    }
    public StateResult(ER userResult, Map<String, String> eventData) {
        this.userResult = userResult;
        this.eventData = eventData == null ? Collections.emptyMap() : eventData;
    }

    public ER getUserResult() {
        return userResult;
    }

    public Map<String, String> getEventData() {
        return eventData;
    }
}
