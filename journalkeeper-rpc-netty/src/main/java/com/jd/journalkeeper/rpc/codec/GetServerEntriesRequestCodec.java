package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.server.GetServerEntriesRequest;
import com.jd.journalkeeper.rpc.server.RequestVoteRequest;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class GetServerEntriesRequestCodec extends GenericPayloadCodec<GetServerEntriesRequest> implements Type {
    @Override
    protected void encodePayload(GetServerEntriesRequest request, ByteBuf buffer) throws Exception {
//        long index, int maxSize
        CodecSupport.encodeLong(buffer, request.getIndex());
        CodecSupport.encodeInt(buffer, request.getMaxSize());
    }

    @Override
    protected GetServerEntriesRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new GetServerEntriesRequest(
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeInt(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.GET_SERVER_ENTRIES_REQUEST;
    }
}
