package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodecFactory;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class PayloadCodeRegsitry {
    public static void register(PayloadCodecFactory payloadCodecFactory) {
        payloadCodecFactory.register(new UpdateClusterStateRequestCodec());
        payloadCodecFactory.register(new UpdateClusterStateResponseCodec());
    }
}
