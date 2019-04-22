package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.AddPullWatchResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class AddPullWatchResponseCodec extends ResponseCodec<AddPullWatchResponse> implements Type {
    @Override
    protected void encodeResponse(AddPullWatchResponse response, ByteBuf buffer) throws Exception {
        //boolean success, long journalIndex, int term, int entryCount
        CodecSupport.encodeLong(buffer, response.getPullWatchId());
        CodecSupport.encodeLong(buffer, response.getPullIntervalMs());
    }

    @Override
    protected AddPullWatchResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new AddPullWatchResponse(
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.ADD_PULL_WATCH_RESPONSE;
    }
}
