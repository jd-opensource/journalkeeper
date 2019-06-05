package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.WatchRequest;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * WatchRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class WatchRequestCodec implements CoordinatingPayloadCodec<WatchRequest> {

    @Override
    public Object decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        byte[] key = CodecSupport.decodeBytes(buffer);
        boolean once = CodecSupport.decodeBoolean(buffer);

        WatchRequest watchRequest = new WatchRequest();
        watchRequest.setKey(key);
        watchRequest.setOnce(once);
        return watchRequest;
    }

    @Override
    public void encode(WatchRequest payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, payload.getKey());
        CodecSupport.encodeBoolean(buffer, payload.isOnce());
    }

    @Override
    public int type() {
        return CoordinatingCommands.WATCH_REQUEST.getType();
    }
}