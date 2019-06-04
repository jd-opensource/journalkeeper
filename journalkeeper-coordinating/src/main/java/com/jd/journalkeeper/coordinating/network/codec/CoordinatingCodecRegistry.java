package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodecFactory;

/**
 * CoordinatingCodecRegistry
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class CoordinatingCodecRegistry {

    public static void register(PayloadCodecFactory payloadCodecFactory) {
        payloadCodecFactory.register(new GetClusterRequestCodec());
        payloadCodecFactory.register(new GetClusterResponseCodec());
        payloadCodecFactory.register(new PutRequestCodec());
        payloadCodecFactory.register(new PutResponseCodec());
        payloadCodecFactory.register(new GetRequestCodec());
        payloadCodecFactory.register(new GetResponseCodec());
    }
}