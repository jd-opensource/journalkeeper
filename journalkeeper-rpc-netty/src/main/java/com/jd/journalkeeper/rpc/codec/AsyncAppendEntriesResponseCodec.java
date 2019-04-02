package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Codec;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class AsyncAppendEntriesResponseCodec extends ResponseCodec<AsyncAppendEntriesResponse> implements Type {
    @Override
    protected void encodeResponse(AsyncAppendEntriesResponse response, ByteBuf buffer) throws Exception {
        //boolean success, long journalIndex, int term, int entryCount
        CodecSupport.encodeBoolean(buffer, response.isSuccess());
        CodecSupport.encodeLong(buffer, response.getJournalIndex());
        CodecSupport.encodeInt(buffer, response.getTerm());
        CodecSupport.encodeInt(buffer, response.getEntryCount());
    }

    @Override
    protected AsyncAppendEntriesResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new AsyncAppendEntriesResponse(
                CodecSupport.decodeBoolean(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeInt(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.ASYNC_APPEND_ENTRIES_RESPONSE;
    }
}
