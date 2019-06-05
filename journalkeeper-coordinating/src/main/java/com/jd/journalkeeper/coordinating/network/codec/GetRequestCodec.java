package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.GetRequest;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * GetRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class GetRequestCodec implements CoordinatingPayloadCodec<GetRequest> {

    @Override
    public GetRequest decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        byte[] key = CodecSupport.decodeBytes(buffer);

        GetRequest getRequest = new GetRequest();
        getRequest.setKey(key);
        return getRequest;
    }

    @Override
    public void encode(GetRequest payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, payload.getKey());
    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_REQUEST.getType();
    }
}