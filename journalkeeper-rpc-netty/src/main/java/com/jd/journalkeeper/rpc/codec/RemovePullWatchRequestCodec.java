package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.RemovePullWatchRequest;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class RemovePullWatchRequestCodec extends GenericPayloadCodec<RemovePullWatchRequest> implements Type {
    @Override
    protected void encodePayload(RemovePullWatchRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, request.getPullWatchId());

    }

    @Override
    protected RemovePullWatchRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new RemovePullWatchRequest(
                CodecSupport.decodeLong(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.REMOVE_PULL_WATCH_REQUEST;
    }
}
