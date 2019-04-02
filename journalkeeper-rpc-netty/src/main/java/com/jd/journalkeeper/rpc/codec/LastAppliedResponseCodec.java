package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.LastAppliedResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

import static com.jd.journalkeeper.rpc.codec.RpcTypes.LAST_APPLIED_RESPONSE;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class LastAppliedResponseCodec extends LeaderResponseCodec<LastAppliedResponse> implements Type {
    @Override
    protected void encodeLeaderResponse(LastAppliedResponse leaderResponse, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, leaderResponse.getLastApplied());
    }

    @Override
    protected LastAppliedResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new LastAppliedResponse(CodecSupport.decodeLong(buffer));
    }

    @Override
    public int type() {
        return LAST_APPLIED_RESPONSE;
    }
}
