package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.ExistRequest;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * ExistRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class ExistRequestCodec implements CoordinatingPayloadCodec<ExistRequest> {

    @Override
    public ExistRequest decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        byte[] key = CodecSupport.decodeBytes(buffer);

        ExistRequest existRequest = new ExistRequest();
        existRequest.setKey(key);
        return existRequest;
    }

    @Override
    public void encode(ExistRequest payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, payload.getKey());
    }

    @Override
    public int type() {
        return CoordinatingCommands.EXIST_REQUEST.getType();
    }
}