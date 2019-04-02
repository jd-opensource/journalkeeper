package com.jd.journalkeeper.rpc.remoting.example;

import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodec;
import com.jd.journalkeeper.rpc.remoting.transport.command.Types;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class StringPayloadCodec implements PayloadCodec<TestHeader, StringPayload> , Types {
    @Override
    public Object decode(TestHeader header, ByteBuf buffer) throws Exception {
        return new StringPayload(CodecSupport.decodeString(buffer));
    }

    @Override
    public void encode(StringPayload payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeString(buffer,payload.getPayload());
    }

    @Override
    public int[] types() {
        return new int[] {-1, 1};
    }
}
