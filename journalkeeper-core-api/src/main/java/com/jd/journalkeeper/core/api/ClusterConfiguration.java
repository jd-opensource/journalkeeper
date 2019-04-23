package com.jd.journalkeeper.core.api;

import java.net.URI;
import java.util.List;
import java.util.Set;

/**
 * Raft集群配置（服务器信息）
 * @author liyue25
 * Date: 2019-03-14
 */
public class ClusterConfiguration {
    private URI leader;
    private List<URI> voters;
    private List<URI> observers;

    public ClusterConfiguration() {}
    public ClusterConfiguration(URI leader, List<URI> voters, List<URI> observers){
        this.leader = leader;
        this.voters = voters;
        this.observers = observers;
    }


    public URI getLeader() {
        return leader;
    }

    public void setLeader(URI leader) {
        this.leader = leader;
    }

    public List<URI> getVoters() {
        return voters;
    }

    public void setVoters(List<URI> voters) {
        this.voters = voters;
    }

    public List<URI> getObservers() {
        return observers;
    }

    public void setObservers(List<URI> observers) {
        this.observers = observers;
    }
}
