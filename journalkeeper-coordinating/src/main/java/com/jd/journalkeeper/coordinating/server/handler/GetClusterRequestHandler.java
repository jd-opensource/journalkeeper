package com.jd.journalkeeper.coordinating.server.handler;

import com.jd.journalkeeper.coordinating.keeper.CoordinatingKeeperServer;
import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.GetClusterRequest;
import com.jd.journalkeeper.coordinating.network.command.GetClusterResponse;
import com.jd.journalkeeper.coordinating.server.CoordinatingContext;
import com.jd.journalkeeper.coordinating.server.config.CoordinatingConfig;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.CommandHandler;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * GetClusterRequestHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class GetClusterRequestHandler implements CommandHandler, Type {

    private CoordinatingConfig config;
    private CoordinatingKeeperServer keeperServer;

    public GetClusterRequestHandler(CoordinatingContext context) {
        this.config = context.getConfig();
        this.keeperServer = context.getKeeperServer();
    }

    @Override
    public Command handle(Transport transport, Command command) {
        GetClusterRequest request = (GetClusterRequest) command.getPayload();
        GetClusterResponse response = new GetClusterResponse();

        URI leader = keeperServer.getLeader();
        List<URI> followers = keeperServer.getCluster();

        if (leader != null) {
            response.setLeader(URI.create(String.format("%s://%s:%s", leader.getScheme(), leader.getHost(), config.getServer().getPort())));
        }

        if (followers != null) {
            List<URI> newFollowers = new ArrayList<>(followers.size());
            for (URI follower : followers) {
                if (leader != null && follower.equals(leader)) {
                    continue;
                }
                newFollowers.add(URI.create(String.format("%s://%s:%s", follower.getScheme(), follower.getHost(), config.getServer().getPort())));
            }
            response.setFollowers(newFollowers);
        }

        return new Command(response);
    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_CLUSTER_REQUEST.getType();
    }
}