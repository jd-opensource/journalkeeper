package com.jd.journalkeeper.rpc.handler;

import com.jd.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFactory;
import com.jd.journalkeeper.rpc.server.ServerRpc;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class ServerRpcCommandHandlerReegistry {
    public static void register(DefaultCommandHandlerFactory factory, ServerRpc serverRpc) {
        factory.register(new UpdateClusterStateHandler(serverRpc));
        factory.register(new LastAppliedHandler(serverRpc));
        factory.register(new QueryClusterStateHandler(serverRpc));
        factory.register(new QueryServerStateHandler(serverRpc));
        factory.register(new QuerySnapshotHandler(serverRpc));
        factory.register(new GetServersHandler(serverRpc));

        factory.register(new AsyncAppendEntriesHandler(serverRpc));
        factory.register(new RequestVoteHandler(serverRpc));
        factory.register(new GetServerEntriesHandler(serverRpc));
        factory.register(new GetServerStateHandler(serverRpc));

    }
}
