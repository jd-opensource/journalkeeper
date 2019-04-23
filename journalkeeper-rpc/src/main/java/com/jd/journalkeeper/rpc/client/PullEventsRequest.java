package com.jd.journalkeeper.rpc.client;

/**
 * RPC 方法
 * {@link ClientServerRpc#pullEvents(PullEventsRequest) pullEvents()}
 * 请求参数
 * @author liyue25
 * Date: 2019-04-22
 */
public class PullEventsRequest {
    private final long pullWatchId;
    private final long ackSequence;

    public PullEventsRequest(long pullWatchId, long ackSequence) {
        this.pullWatchId = pullWatchId;
        this.ackSequence = ackSequence;
    }

    /**
     * 获取监听ID
     * @return 监听ID
     */
    public long getPullWatchId() {
        return pullWatchId;
    }

    /**
     * 获取确认位置，用于确认已收到的事件。
     * @return 确认位置。如果确认位置小于0，则本次请求不进行确认事件操作。
     */
    public long getAckSequence() {
        return ackSequence;
    }
}
