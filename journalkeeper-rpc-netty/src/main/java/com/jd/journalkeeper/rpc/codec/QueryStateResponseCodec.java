package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.QueryStateResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Types;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class QueryStateResponseCodec extends LeaderResponseCodec<QueryStateResponse> implements Types {
    @Override
    protected void encodeLeaderResponse(QueryStateResponse response, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, response.getLastApplied());
        CodecSupport.encodeBytes(buffer, response.getResult());
    }

    @Override
    protected QueryStateResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        long lastApplied = CodecSupport.decodeLong(buffer);
        byte [] result = CodecSupport.decodeBytes(buffer);
        return new QueryStateResponse(result, lastApplied);
    }


    @Override
    public int[] types() {
        return new int[] {
                RpcTypes.QUERY_CLUSTER_STATE_RESPONSE,
                RpcTypes.QUERY_SERVER_STATE_RESPONSE,
                RpcTypes.QUERY_SNAPSHOT_RESPONSE
        };
    }
}
