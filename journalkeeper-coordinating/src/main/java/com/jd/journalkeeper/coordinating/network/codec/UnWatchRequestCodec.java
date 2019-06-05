package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.UnWatchRequest;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * UnWatchRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class UnWatchRequestCodec implements CoordinatingPayloadCodec<UnWatchRequest> {

    @Override
    public UnWatchRequest decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        byte[] key = CodecSupport.decodeBytes(buffer);

        UnWatchRequest unWatchRequest = new UnWatchRequest();
        unWatchRequest.setKey(key);
        return unWatchRequest;
    }

    @Override
    public void encode(UnWatchRequest payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, payload.getKey());
    }

    @Override
    public int type() {
        return CoordinatingCommands.UN_WATCH_REQUEST.getType();
    }
}