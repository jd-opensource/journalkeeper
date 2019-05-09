package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.core.api.ResponseConfig;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateRequest;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class UpdateClusterStateRequestCodec extends GenericPayloadCodec<UpdateClusterStateRequest> implements Type {
    @Override
    protected void encodePayload(UpdateClusterStateRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, request.getEntry());
        CodecSupport.encodeShort(buffer, request.getPartition());
        CodecSupport.encodeShort(buffer, request.getBatchSize());
        CodecSupport.encodeByte(buffer, (byte) request.getResponseConfig().value());
    }

    @Override
    protected UpdateClusterStateRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new UpdateClusterStateRequest(
                CodecSupport.decodeBytes(buffer),
                CodecSupport.decodeShort(buffer),
                CodecSupport.decodeShort(buffer),
                ResponseConfig.valueOf(CodecSupport.decodeByte(buffer)));
    }

    @Override
    public int type() {
        return RpcTypes.UPDATE_CLUSTER_STATE_REQUEST;
    }
}
