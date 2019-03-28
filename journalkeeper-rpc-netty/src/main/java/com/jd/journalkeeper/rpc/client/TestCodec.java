package com.jd.journalkeeper.rpc.client;


import com.jd.journalkeeper.rpc.transport.codec.*;
import com.jd.journalkeeper.rpc.transport.exception.TransportException;
import io.netty.buffer.ByteBuf;

/**
 * JMQCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/10/22
 */
public class TestCodec implements Codec {

    private Codec headerCodec;
    private PayloadCodecFactory payloadCodecFactory;

    private Decoder decoder;
    private Encoder encoder;

    public TestCodec() {
        PayloadCodecFactory payloadCodecFactory = new PayloadCodecFactory();
        payloadCodecFactory.register(1, new StringPayloadCodec()); // 请求
        payloadCodecFactory.register(-1, new StringPayloadCodec());// 响应
//        JMQPayloadCodecRegistry.register(payloadCodecFactory);
        this.headerCodec = new TestHeaderCodec();
        this.payloadCodecFactory = payloadCodecFactory;
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

    public PayloadCodecFactory getPayloadCodecFactory() {
        return payloadCodecFactory;
    }
}
