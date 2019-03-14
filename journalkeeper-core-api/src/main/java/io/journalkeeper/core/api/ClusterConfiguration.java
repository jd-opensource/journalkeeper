package io.journalkeeper.core.api;

import java.net.URI;
import java.util.Set;

/**
 * Raft集群配置（服务器信息）
 * @author liyue25
 * Date: 2019-03-14
 */
public class ClusterConfiguration {
    private URI leader;
    private Set<URI> voters;
    private Set<URI> observers;

    public URI getLeader() {
        return leader;
    }

    public void setLeader(URI leader) {
        this.leader = leader;
    }

    public Set<URI> getVoters() {
        return voters;
    }

    public void setVoters(Set<URI> voters) {
        this.voters = voters;
    }

    public Set<URI> getObservers() {
        return observers;
    }

    public void setObservers(Set<URI> observers) {
        this.observers = observers;
    }
}
