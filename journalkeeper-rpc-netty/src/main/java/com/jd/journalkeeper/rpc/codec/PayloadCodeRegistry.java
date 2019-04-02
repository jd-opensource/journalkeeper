package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodecFactory;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class PayloadCodeRegistry {
    public static void register(PayloadCodecFactory payloadCodecFactory) {
        payloadCodecFactory.register(new UpdateClusterStateRequestCodec());
        payloadCodecFactory.register(new UpdateClusterStateResponseCodec());
        payloadCodecFactory.register(new LastAppliedRequestCodec());
        payloadCodecFactory.register(new LastAppliedResponseCodec());
        payloadCodecFactory.register(new QueryStateRequestCodec());
        payloadCodecFactory.register(new QueryStateResponseCodec());
        payloadCodecFactory.register(new GetServersRequestCodec());
        payloadCodecFactory.register(new GetServersResponseCodec());
    }
}
