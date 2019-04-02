package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import com.jd.journalkeeper.rpc.server.AsyncAppendEntriesResponse;
import com.jd.journalkeeper.rpc.server.RequestVoteResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class RequestVoteResponseCodec extends ResponseCodec<RequestVoteResponse> implements Type {
    @Override
    protected void encodeResponse(RequestVoteResponse response, ByteBuf buffer) throws Exception {
        // int term, boolean voteGranted
        CodecSupport.encodeInt(buffer, response.getTerm());
        CodecSupport.encodeBoolean(buffer, response.isVoteGranted());
    }

    @Override
    protected RequestVoteResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new RequestVoteResponse(
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeBoolean(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.REQUEST_VOTE_RESPONSE;
    }
}
