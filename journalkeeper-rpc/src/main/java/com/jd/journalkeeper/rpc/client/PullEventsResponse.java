package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.StatusCode;
import com.jd.journalkeeper.utils.event.PullEvent;

import java.util.Collections;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-04-22
 */
public class PullEventsResponse extends BaseResponse {
    private final List<PullEvent> pullEvents;

    public PullEventsResponse(List<PullEvent> pullEvents) {
        super(StatusCode.SUCCESS);
        this.pullEvents = pullEvents;
    }

    public List<PullEvent> getPullEvents() {
        return pullEvents;
    }

    public PullEventsResponse(Throwable throwable) {
        super(throwable);
        this.pullEvents = Collections.emptyList();
    }
}
