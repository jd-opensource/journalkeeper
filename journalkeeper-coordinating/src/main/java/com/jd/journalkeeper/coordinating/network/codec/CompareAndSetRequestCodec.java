package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.CompareAndSetRequest;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * CompareAndSetRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class CompareAndSetRequestCodec implements CoordinatingPayloadCodec<CompareAndSetRequest> {

    @Override
    public CompareAndSetRequest decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        byte[] key = CodecSupport.decodeBytes(buffer);
        byte[] expect = CodecSupport.decodeBytes(buffer);
        byte[] update = CodecSupport.decodeBytes(buffer);

        CompareAndSetRequest compareAndSetRequest = new CompareAndSetRequest();
        compareAndSetRequest.setKey(key);
        compareAndSetRequest.setExpect(expect);
        compareAndSetRequest.setUpdate(update);
        return compareAndSetRequest;
    }

    @Override
    public void encode(CompareAndSetRequest payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, payload.getKey());
        CodecSupport.encodeBytes(buffer, payload.getExpect());
        CodecSupport.encodeBytes(buffer, payload.getUpdate());
    }

    @Override
    public int type() {
        return CoordinatingCommands.COMPARE_AND_SET_REQUEST.getType();
    }
}
