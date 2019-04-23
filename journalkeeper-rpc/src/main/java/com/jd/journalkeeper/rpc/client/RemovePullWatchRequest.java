package com.jd.journalkeeper.rpc.client;

/**
 * RPC 方法
 * {@link ClientServerRpc#removePullWatch(RemovePullWatchRequest)}
 * 请求参数
 * @author liyue25
 * Date: 2019-04-22
 */
public class RemovePullWatchRequest {
    private final long pullWatchId;

    public RemovePullWatchRequest(long pullWatchId) {
        this.pullWatchId = pullWatchId;
    }

    /**
     * 获取监听ID
     * @return 监听ID
     */
    public long getPullWatchId() {
        return pullWatchId;
    }
}
