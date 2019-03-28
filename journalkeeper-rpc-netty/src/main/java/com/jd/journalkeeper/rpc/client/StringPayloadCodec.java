package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.remoting.serialize.SerializeSupport;
import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodec;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class StringPayloadCodec implements PayloadCodec<TestHeader, StringPayload> {
    @Override
    public Object decode(TestHeader header, ByteBuf buffer) throws Exception {
        return new StringPayload(SerializeSupport.readString(buffer));
    }

    @Override
    public void encode(StringPayload payload, ByteBuf buffer) throws Exception {
        SerializeSupport.writeString(buffer,payload.getPayload());
    }
}
