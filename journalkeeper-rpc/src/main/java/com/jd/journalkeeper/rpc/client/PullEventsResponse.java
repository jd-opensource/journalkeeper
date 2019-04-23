package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.StatusCode;
import com.jd.journalkeeper.utils.event.PullEvent;

import java.util.Collections;
import java.util.List;

/**
 * RPC 方法
 * {@link ClientServerRpc#pullEvents(PullEventsRequest) pullEvents{}}
 * 返回响应。
 *
 * 返回从上次ack 的序号至今的所有事件，保证事件有序。
 * 如果没有事件返回长度为0的List。
 *
 * StatusCode:
 * StatusCode.PULL_WATCH_ID_NOT_EXISTS: 监听ID不存在。
 * @author liyue25
 * Date: 2019-04-22
 */
public class PullEventsResponse extends BaseResponse {
    private final List<PullEvent> pullEvents;

    public PullEventsResponse(List<PullEvent> pullEvents) {
        if(null != pullEvents) {
            setStatusCode(StatusCode.SUCCESS);
        } else {
            setStatusCode(StatusCode.PULL_WATCH_ID_NOT_EXISTS);
        }
        this.pullEvents = pullEvents;
    }

    /**
     * 返回的事件列表
     * @return 返回的事件列表
     */
    public List<PullEvent> getPullEvents() {
        return pullEvents;
    }

    public PullEventsResponse(Throwable throwable) {
        super(throwable);
        this.pullEvents = Collections.emptyList();
    }
}
