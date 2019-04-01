package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.StatusCode;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.SerializeSupport;
import io.netty.buffer.ByteBuf;

import java.net.URI;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public abstract class ResponseCodec<R extends BaseResponse> extends GenericPayloadCodec<R> {
    @Override
    public final R decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        R response = decodeResponse(header, buffer);
        response.setStatusCode(StatusCode.valueOf(header.getStatus()));
        response.setError(header.getError());
        return response;
    }

    @Override
    public final void encodePayload(R response, ByteBuf buffer) throws Exception {
        encodeResponse(response, buffer);
    }

    protected abstract void encodeResponse(R leaderResponse, ByteBuf buffer) throws Exception;
    protected abstract R decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception;
}
