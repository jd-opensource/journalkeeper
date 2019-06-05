package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.RemoveRequest;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * RemoveRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class RemoveRequestCodec implements CoordinatingPayloadCodec<RemoveRequest> {

    @Override
    public RemoveRequest decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        byte[] key = CodecSupport.decodeBytes(buffer);

        RemoveRequest removeRequest = new RemoveRequest();
        removeRequest.setKey(key);
        return removeRequest;
    }

    @Override
    public void encode(RemoveRequest payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, payload.getKey());
    }

    @Override
    public int type() {
        return CoordinatingCommands.REMOVE_REQUEST.getType();
    }
}