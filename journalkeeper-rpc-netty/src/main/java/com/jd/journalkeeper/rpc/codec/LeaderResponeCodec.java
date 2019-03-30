package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.LeaderResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.SerializeSupport;
import io.netty.buffer.ByteBuf;

import java.net.URI;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public abstract class LeaderResponeCodec<R extends LeaderResponse> extends GenericPayloadCodec<R> {
    @Override
    public final R decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        R response = decodeLeaderResponse(header, buffer);
        String leaderStr = SerializeSupport.readString(buffer);
        if(leaderStr != null && leaderStr.length() > 0) {
            response.setLeader(URI.create(leaderStr));
        }
        return response;
    }

    @Override
    public final void encodePayload(R response, ByteBuf buffer) throws Exception {

        encodeLeaderResponse(response, buffer);
        SerializeSupport.writeString(buffer,response.getLeader().toString());

    }

    protected abstract void encodeLeaderResponse(R leaderResponse, ByteBuf buffer) throws Exception;
    protected abstract R decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception;
}
