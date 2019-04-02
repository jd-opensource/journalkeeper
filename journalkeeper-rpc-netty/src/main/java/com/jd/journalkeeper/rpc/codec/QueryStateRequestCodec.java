package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.QueryStateRequest;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateRequest;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.command.Types;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class QueryStateRequestCodec extends GenericPayloadCodec<QueryStateRequest> implements Types {
    @Override
    protected void encodePayload(QueryStateRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, request.getIndex());
        CodecSupport.encodeBytes(buffer, request.getQuery());
    }

    @Override
    protected QueryStateRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        long index = CodecSupport.decodeLong(buffer);
        byte [] query = CodecSupport.decodeBytes(buffer);
        return new QueryStateRequest(query, index);
    }

    @Override
    public int[] types() {
        return new int[] {
                RpcTypes.QUERY_CLUSTER_STATE_REQUEST,
                RpcTypes.QUERY_SERVER_STATE_REQUEST,
                RpcTypes.QUERY_SNAPSHOT_REQUEST
        };
    }
}
