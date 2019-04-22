package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.RemovePullWatchResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class RemovePullWatchResponseCodec extends ResponseCodec<RemovePullWatchResponse> implements Type {
    @Override
    protected void encodeResponse(RemovePullWatchResponse response, ByteBuf buffer) throws Exception {
        //boolean success, long journalIndex, int term, int entryCount
    }

    @Override
    protected RemovePullWatchResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new RemovePullWatchResponse();
    }

    @Override
    public int type() {
        return RpcTypes.REMOVE_PULL_WATCH_RESPONSE;
    }
}
