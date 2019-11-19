package io.journalkeeper.monitor;

import java.net.URI;

/**
 * Leader节点上记录的Follower信息
 * @author LiYue
 * Date: 2019/11/19
 */
public class LeaderFollowerMonitorInfo {
    // LEADER的从节点URI	当前LEADER节点记录的FOLLOWER节点的URI
    private URI uri = null;
    // 下一次复制索引序号	需要发给FOLLOWER的下一个日志条目的索引（初始化为领导人上一条日志的索引值 +1）
    private long nextIndex = -1L;
    // 已复制索引序号	已经复制到该FOLLOWER的日志的最高索引值（从 0 开始递增）
    private long matchIndex = -1L;
    // 在途复制请求索引起始值	所有在途的日志复制请求中日志位置的最小值（初始化为nextIndex）
    private long repStartIndex = -1L;
    // 心跳响应时间	上次从FOLLOWER收到心跳（asyncAppendEntries）成功响应的时间戳
    private long lastHeartbeatResponseTime = -1L;
    // 心跳发送时间	上次发给FOLLOWER心跳（asyncAppendEntries）的时间戳
    private long lastHeartbeatRequestTime = -1L;

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    public long getNextIndex() {
        return nextIndex;
    }

    public void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(long matchIndex) {
        this.matchIndex = matchIndex;
    }

    public long getRepStartIndex() {
        return repStartIndex;
    }

    public void setRepStartIndex(long repStartIndex) {
        this.repStartIndex = repStartIndex;
    }

    public long getLastHeartbeatResponseTime() {
        return lastHeartbeatResponseTime;
    }

    public void setLastHeartbeatResponseTime(long lastHeartbeatResponseTime) {
        this.lastHeartbeatResponseTime = lastHeartbeatResponseTime;
    }

    public long getLastHeartbeatRequestTime() {
        return lastHeartbeatRequestTime;
    }

    public void setLastHeartbeatRequestTime(long lastHeartbeatRequestTime) {
        this.lastHeartbeatRequestTime = lastHeartbeatRequestTime;
    }

    @Override
    public String toString() {
        return "LeaderFollowerMonitorInfo{" +
                "uri=" + uri +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                ", repStartIndex=" + repStartIndex +
                ", lastHeartbeatResponseTime=" + lastHeartbeatResponseTime +
                ", lastHeartbeatRequestTime=" + lastHeartbeatRequestTime +
                '}';
    }
}
