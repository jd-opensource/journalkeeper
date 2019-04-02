package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.payload.VoidPayload;
import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodec;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class VoidPayloadCodec implements PayloadCodec<JournalKeeperHeader, VoidPayload> {

    @Override
    public Object decode(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return null;
    }

    @Override
    public void encode(VoidPayload payload, ByteBuf buffer) throws Exception {

    }
}
