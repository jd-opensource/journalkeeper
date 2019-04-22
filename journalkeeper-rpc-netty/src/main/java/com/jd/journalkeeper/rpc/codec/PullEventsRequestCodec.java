package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.PullEventsRequest;
import com.jd.journalkeeper.rpc.client.RemovePullWatchRequest;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class PullEventsRequestCodec extends GenericPayloadCodec<PullEventsRequest> implements Type {
    @Override
    protected void encodePayload(PullEventsRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, request.getPullWatchId());
        CodecSupport.encodeLong(buffer, request.getAckSequence());

    }

    @Override
    protected PullEventsRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new PullEventsRequest(
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.PULL_EVENTS_REQUEST;
    }
}
