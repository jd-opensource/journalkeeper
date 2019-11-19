package io.journalkeeper.monitor;

import io.journalkeeper.utils.state.StateServer;

import java.util.Collection;

/**
 * @author LiYue
 * Date: 2019/11/19
 */
public class LeaderMonitorInfo {
    // 当前节点LEADER状态	枚举:
    //CREATED, STARTING, RUNNING, STOPPING, STOPPED, START_FAILED, STOP_FAILED
    private StateServer.ServerState state = null;
    // 请求队列排队数	写入请求队列当前排队数量。所有写入请求先进入这个队列然后再异步串行处理，如何这个数量持续保持高位，说明写入积压。
    private int requestQueueSize = -1;
    // 是否可写	正常情况为true可写，管理员可以通过调用接口禁止写入。
    private boolean writeEnabled = false;
    // 从节点信息
    private Collection<LeaderFollowerMonitorInfo> followers;

    public StateServer.ServerState getState() {
        return state;
    }

    public void setState(StateServer.ServerState state) {
        this.state = state;
    }

    public int getRequestQueueSize() {
        return requestQueueSize;
    }

    public void setRequestQueueSize(int requestQueueSize) {
        this.requestQueueSize = requestQueueSize;
    }

    public boolean isWriteEnabled() {
        return writeEnabled;
    }

    public void setWriteEnabled(boolean writeEnabled) {
        this.writeEnabled = writeEnabled;
    }

    public Collection<LeaderFollowerMonitorInfo> getFollowers() {
        return followers;
    }

    public void setFollowers(Collection<LeaderFollowerMonitorInfo> followers) {
        this.followers = followers;
    }

    @Override
    public String toString() {
        return "LeaderMonitorInfo{" +
                "state=" + state +
                ", requestQueueSize=" + requestQueueSize +
                ", writeEnabled=" + writeEnabled +
                ", followers=" + followers +
                '}';
    }
}
