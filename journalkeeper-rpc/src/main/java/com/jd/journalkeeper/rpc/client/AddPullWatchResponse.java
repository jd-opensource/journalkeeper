package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.StatusCode;

/**
 * @author liyue25
 * Date: 2019-04-19
 */
public class AddPullWatchResponse extends BaseResponse {
    private final long pullWatchId;
    private final long pullIntervalMs;

    public AddPullWatchResponse(long pullWatchId, long pullIntervalMs) {
        super(StatusCode.SUCCESS);
        this.pullWatchId = pullWatchId;
        this.pullIntervalMs = pullIntervalMs;
    }

    public AddPullWatchResponse(Throwable throwable) {
        super(throwable);
        this.pullIntervalMs = -1L;
        this.pullWatchId = -1L;
    }

    public long getPullWatchId() {
        return pullWatchId;
    }

    public long getPullIntervalMs() {
        return pullIntervalMs;
    }
}
