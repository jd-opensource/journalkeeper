package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.UpdateClusterStateResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class UpdateClusterStateResponseCodec extends LeaderResponseCodec<UpdateClusterStateResponse> implements Type {
    @Override
    protected void encodeLeaderResponse(UpdateClusterStateResponse leaderResponse, ByteBuf buffer) throws Exception {

    }

    @Override
    protected UpdateClusterStateResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new UpdateClusterStateResponse();
    }

    @Override
    public int type() {
        return RpcTypes.UPDATE_CLUSTER_STATE_RESPONSE;
    }
}
