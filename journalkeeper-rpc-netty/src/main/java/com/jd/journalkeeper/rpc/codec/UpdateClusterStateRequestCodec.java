package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.UpdateClusterStateRequest;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.SerializeSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class UpdateClusterStateRequestCodec extends GenericPayloadCodec<UpdateClusterStateRequest> implements Type {
    @Override
    protected void encodePayload(UpdateClusterStateRequest request, ByteBuf buffer) throws Exception {
        SerializeSupport.writeBytes(buffer, request.getEntry());
    }

    @Override
    protected UpdateClusterStateRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new UpdateClusterStateRequest(SerializeSupport.readBytes(buffer));
    }

    @Override
    public int type() {
        return RpcTypes.UPDATE_CLUSTER_STATE_REQUEST;
    }
}
