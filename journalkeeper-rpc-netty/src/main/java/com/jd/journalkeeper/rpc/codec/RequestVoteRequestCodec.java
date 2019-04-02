package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesRequest;
import com.jd.journalkeeper.rpc.server.RequestVoteRequest;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class RequestVoteRequestCodec extends GenericPayloadCodec<RequestVoteRequest> implements Type {
    @Override
    protected void encodePayload(RequestVoteRequest request, ByteBuf buffer) throws Exception {
//        int term, URI candidate, long lastLogIndex, int lastLogTerm
        CodecSupport.encodeInt(buffer, request.getTerm());
        CodecSupport.encodeUri(buffer, request.getCandidate());
        CodecSupport.encodeLong(buffer, request.getLastLogIndex());
        CodecSupport.encodeInt(buffer, request.getLastLogTerm());
    }

    @Override
    protected RequestVoteRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new RequestVoteRequest(
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeUri(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeInt(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.REQUEST_VOTE_REQUEST;
    }
}
