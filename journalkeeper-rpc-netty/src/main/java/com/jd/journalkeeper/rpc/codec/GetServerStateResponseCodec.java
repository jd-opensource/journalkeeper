package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.server.GetServerStateResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class GetServerStateResponseCodec extends ResponseCodec<GetServerStateResponse> implements Type {

    @Override
    public int type() {
        return RpcTypes.GET_SERVER_STATE_RESPONSE;
    }

    @Override
    protected void encodeResponse(GetServerStateResponse response, ByteBuf buffer) throws Exception {
        //long lastIncludedIndex, int lastIncludedTerm, long offset, byte[] data, boolean done
        CodecSupport.encodeLong(buffer, response.getLastIncludedIndex());
        CodecSupport.encodeInt(buffer, response.getLastIncludedTerm());
        CodecSupport.encodeLong(buffer, response.getOffset());
        CodecSupport.encodeBytes(buffer, response.getData());
        CodecSupport.encodeBoolean(buffer, response.isDone());

    }

    @Override
    protected GetServerStateResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new GetServerStateResponse(
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeBytes(buffer),
                CodecSupport.decodeBoolean(buffer)
        );
    }
}
