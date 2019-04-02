package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Decoder;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Encoder;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.remoting.transport.exception.TransportException;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class AsyncAppendEntriesRequestCodec extends GenericPayloadCodec<AsyncAppendEntriesRequest> implements Type {
    @Override
    protected void encodePayload(AsyncAppendEntriesRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeInt(buffer, request.getTerm());
        CodecSupport.encodeUri(buffer, request.getLeader());
        CodecSupport.encodeLong(buffer, request.getPrevLogIndex());
        CodecSupport.encodeInt(buffer, request.getPrevLogTerm());
        CodecSupport.encodeList(buffer, request.getEntries(),
                (obj, buffer1) -> CodecSupport.encodeBytes(buffer1, (byte []) obj));
        CodecSupport.encodeLong(buffer, request.getLeaderCommit());

    }

    @Override
    protected AsyncAppendEntriesRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new AsyncAppendEntriesRequest(
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeUri(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeList(buffer, CodecSupport::decodeBytes),
                CodecSupport.decodeLong(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.ASYNC_APPEND_ENTRIES_REQUEST;
    }
}
