package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.rpc.remoting.transport.codec.Codec;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Decoder;
import com.jd.journalkeeper.rpc.remoting.transport.codec.DefaultDecoder;
import com.jd.journalkeeper.rpc.remoting.transport.codec.DefaultEncoder;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Encoder;
import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodecFactory;
import com.jd.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.netty.buffer.ByteBuf;

/**
 * CoordinatingCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class CoordinatingCodec implements Codec {

    private Codec headerCodec;
    private Decoder decoder;
    private Encoder encoder;

    public CoordinatingCodec() {
        PayloadCodecFactory payloadCodecFactory = new PayloadCodecFactory();
        CoordinatingCodecRegistry.register(payloadCodecFactory);
        this.headerCodec = new CoordinatingHeaderCodec();
        this.decoder = new DefaultDecoder(headerCodec, payloadCodecFactory);
        this.encoder = new DefaultEncoder(headerCodec, payloadCodecFactory);
    }

    @Override
    public Object decode(ByteBuf buffer) throws TransportException.CodecException {
        return decoder.decode(buffer);
    }

    @Override
    public void encode(Object obj, ByteBuf buffer) throws TransportException.CodecException {
        encoder.encode(obj, buffer);
    }
}