package com.jd.journalkeeper.coordinating.network.command;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;

import java.net.URI;
import java.util.List;

/**
 * GetClusterResponse
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class GetClusterResponse implements CoordinatingPayload {

    private URI leader;
    private List<URI> followers;

    public URI getLeader() {
        return leader;
    }

    public void setLeader(URI leader) {
        this.leader = leader;
    }

    public void setFollowers(List<URI> followers) {
        this.followers = followers;
    }

    public List<URI> getFollowers() {
        return followers;
    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_CLUSTER_RESPONSE.getType();
    }
}