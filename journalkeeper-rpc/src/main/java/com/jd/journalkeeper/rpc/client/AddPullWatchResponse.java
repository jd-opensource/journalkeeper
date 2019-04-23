package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.StatusCode;

/**
 * RPC 方法
 * {@link ClientServerRpc#addPullWatch() addPullWatch()}
 * 返回响应。
 *
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

    /**
     * 监听ID
     * @return 监听ID
     */
    public long getPullWatchId() {
        return pullWatchId;
    }

    /**
     * 获取拉取监听事件的时间间隔。
     * @return 监听时间间隔，单位毫秒。
     */
    public long getPullIntervalMs() {
        return pullIntervalMs;
    }
}
