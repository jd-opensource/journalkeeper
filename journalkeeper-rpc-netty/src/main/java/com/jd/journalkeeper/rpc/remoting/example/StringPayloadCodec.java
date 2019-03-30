package com.jd.journalkeeper.rpc.remoting.example;

import com.jd.journalkeeper.rpc.remoting.serialize.SerializeSupport;
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
        return new StringPayload(SerializeSupport.readString(buffer));
    }

    @Override
    public void encode(StringPayload payload, ByteBuf buffer) throws Exception {
        SerializeSupport.writeString(buffer,payload.getPayload());
    }

    @Override
    public int[] types() {
        return new int[] {-1, 1};
    }
}
