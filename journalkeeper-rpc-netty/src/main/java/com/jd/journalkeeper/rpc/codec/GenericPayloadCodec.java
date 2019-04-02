package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.payload.GenericPayload;
import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodec;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public abstract class GenericPayloadCodec<P> implements PayloadCodec<JournalKeeperHeader, GenericPayload<P>> {
    @Override
    public final Object decode(JournalKeeperHeader header, ByteBuf buffer) throws Exception {

        return new GenericPayload<>(decodePayload(header, buffer));
    }

    @Override
    public final void encode(GenericPayload<P> payload, ByteBuf buffer) throws Exception {
        encodePayload(payload.getPayload(), buffer);
    }

    protected abstract void encodePayload(P payload, ByteBuf buffer) throws Exception;
    protected abstract P decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception;
}
