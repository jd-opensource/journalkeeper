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
public abstract class LeaderResponseCodec<R extends LeaderResponse> extends ResponseCodec<R> {
    @Override
    protected void encodeResponse(R response, ByteBuf buffer) throws Exception {
        encodeLeaderResponse(response, buffer);
        SerializeSupport.writeString(buffer,response.getLeader() == null ? null: response.getLeader().toString());
    }

    @Override
    protected R decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        R response = decodeLeaderResponse(header, buffer);
        String leaderStr = SerializeSupport.readString(buffer);
        if(leaderStr.length() > 0) {
            response.setLeader(URI.create(leaderStr));
        }
        return response;
    }

    protected abstract void encodeLeaderResponse(R leaderResponse, ByteBuf buffer) throws Exception;
    protected abstract R decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception;
}
