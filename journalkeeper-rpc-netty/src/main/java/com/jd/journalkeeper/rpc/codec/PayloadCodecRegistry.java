package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.remoting.transport.codec.PayloadCodecFactory;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class PayloadCodecRegistry {
    public static void register(PayloadCodecFactory payloadCodecFactory) {
        payloadCodecFactory.register(new UpdateClusterStateRequestCodec());
        payloadCodecFactory.register(new UpdateClusterStateResponseCodec());
        payloadCodecFactory.register(new LastAppliedRequestCodec());
        payloadCodecFactory.register(new LastAppliedResponseCodec());
        payloadCodecFactory.register(new QueryStateRequestCodec());
        payloadCodecFactory.register(new QueryStateResponseCodec());
        payloadCodecFactory.register(new GetServersRequestCodec());
        payloadCodecFactory.register(new GetServersResponseCodec());
        payloadCodecFactory.register(new AddPullWatchRequestCodec());
        payloadCodecFactory.register(new AddPullWatchResponseCodec());
        payloadCodecFactory.register(new RemovePullWatchRequestCodec());
        payloadCodecFactory.register(new RemovePullWatchResponseCodec());
        payloadCodecFactory.register(new PullEventsRequestCodec());
        payloadCodecFactory.register(new PullEventsResponseCodec());


        payloadCodecFactory.register(new AsyncAppendEntriesRequestCodec());
        payloadCodecFactory.register(new AsyncAppendEntriesResponseCodec());
        payloadCodecFactory.register(new RequestVoteRequestCodec());
        payloadCodecFactory.register(new RequestVoteResponseCodec());
        payloadCodecFactory.register(new GetServerEntriesRequestCodec());
        payloadCodecFactory.register(new GetServerEntriesResponseCodec());
        payloadCodecFactory.register(new GetServerStateRequestCodec());
        payloadCodecFactory.register(new GetServerStateResponseCodec());

    }
}
