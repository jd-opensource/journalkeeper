package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.server.GetServerStateRequest;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class GetServerStateRequestCodec extends GenericPayloadCodec<GetServerStateRequest> implements Type {
    @Override
    protected void encodePayload(GetServerStateRequest request, ByteBuf buffer) throws Exception {
        // long lastIncludedIndex, long offset
        CodecSupport.encodeLong(buffer, request.getLastIncludedIndex());
        CodecSupport.encodeLong(buffer, request.getOffset());
    }

    @Override
    protected GetServerStateRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {

        return new GetServerStateRequest(
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.GET_SERVER_STATE_REQUEST;
    }
}

