package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.PutRequest;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * PutRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class PutRequestCodec implements CoordinatingPayloadCodec<PutRequest> {

    @Override
    public PutRequest decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        byte[] key = CodecSupport.decodeBytes(buffer);
        byte[] value = CodecSupport.decodeBytes(buffer);

        PutRequest putRequest = new PutRequest();
        putRequest.setKey(key);
        putRequest.setValue(value);
        return putRequest;
    }

    @Override
    public void encode(PutRequest payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, payload.getKey());
        CodecSupport.encodeBytes(buffer, payload.getValue());
    }

    @Override
    public int type() {
        return CoordinatingCommands.PUT_REQUEST.getType();
    }
}