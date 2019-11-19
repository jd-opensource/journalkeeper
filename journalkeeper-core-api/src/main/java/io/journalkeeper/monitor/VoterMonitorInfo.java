package io.journalkeeper.monitor;

import io.journalkeeper.core.api.VoterState;

import java.net.URI;

/**
 * @author LiYue
 * Date: 2019/11/19
 */
public class VoterMonitorInfo {
    // 选举任期
    private int term = -1;
    // 候选人状态	枚举：LEADER, FOLLOWER, CANDIDATE
    private VoterState state = null;
    // 投票候选人	在当前任期内投票给了哪个候选人，如果未投票可以为NULL。
    private URI lastVote = null;
    // 选举超时	单位为：毫秒（ms）
    private long electionTimeout = -1L;
    // 下次发起选举的时间	仅当voter.state为CANDIDATE的时候有效
    private long nextElectionTime = -1L;
    // 上次心跳时间	记录的上次从LEADER收到的心跳时间
    private long lastHeartbeat = -1L;
    // 推荐LEADER
    private URI preferredLeader = null;
    // LEADER监控信息
    private  LeaderMonitorInfo leader = null;
    // FOLLOWER监控信息
    private FollowerMonitorInfo follower = null;

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public VoterState getState() {
        return state;
    }

    public void setState(VoterState state) {
        this.state = state;
    }

    public URI getLastVote() {
        return lastVote;
    }

    public void setLastVote(URI lastVote) {
        this.lastVote = lastVote;
    }

    public long getElectionTimeout() {
        return electionTimeout;
    }

    public void setElectionTimeout(long electionTimeout) {
        this.electionTimeout = electionTimeout;
    }

    public long getNextElectionTime() {
        return nextElectionTime;
    }

    public void setNextElectionTime(long nextElectionTime) {
        this.nextElectionTime = nextElectionTime;
    }

    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(long lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public URI getPreferredLeader() {
        return preferredLeader;
    }

    public void setPreferredLeader(URI preferredLeader) {
        this.preferredLeader = preferredLeader;
    }

    public LeaderMonitorInfo getLeader() {
        return leader;
    }

    public void setLeader(LeaderMonitorInfo leader) {
        this.leader = leader;
    }

    public FollowerMonitorInfo getFollower() {
        return follower;
    }

    public void setFollower(FollowerMonitorInfo follower) {
        this.follower = follower;
    }

    @Override
    public String toString() {
        return "VoterMonitorInfo{" +
                "term=" + term +
                ", state=" + state +
                ", lastVote=" + lastVote +
                ", electionTimeout=" + electionTimeout +
                ", nextElectionTime=" + nextElectionTime +
                ", lastHeartbeat=" + lastHeartbeat +
                ", preferredLeader=" + preferredLeader +
                ", leader=" + leader +
                ", follower=" + follower +
                '}';
    }
}
