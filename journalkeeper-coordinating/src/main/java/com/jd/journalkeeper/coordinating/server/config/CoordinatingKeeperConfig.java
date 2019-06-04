package com.jd.journalkeeper.coordinating.server.config;

import com.jd.journalkeeper.core.api.RaftServer;

import java.net.URI;
import java.util.List;

/**
 * CoordinatingKeeperConfig
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
public class CoordinatingKeeperConfig {

    private URI current;
    private List<URI> cluster;
    private RaftServer.Roll role;

    public URI getCurrent() {
        return current;
    }

    public void setCurrent(URI current) {
        this.current = current;
    }

    public List<URI> getCluster() {
        return cluster;
    }

    public void setCluster(List<URI> cluster) {
        this.cluster = cluster;
    }

    public void setRole(RaftServer.Roll role) {
        this.role = role;
    }

    public RaftServer.Roll getRole() {
        return role;
    }

    @Override
    public String toString() {
        return "CoordinatingKeeperConfig{" +
                "current=" + current +
                ", cluster=" + cluster +
                ", role=" + role +
                '}';
    }
}