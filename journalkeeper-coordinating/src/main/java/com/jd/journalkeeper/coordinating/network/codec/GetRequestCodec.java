package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.GetRequest;
import io.netty.buffer.ByteBuf;

/**
 * GetRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class GetRequestCodec implements CoordinatingPayloadCodec<GetRequest> {

    @Override
    public GetRequest decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        return null;
    }

    @Override
    public void encode(GetRequest payload, ByteBuf buffer) throws Exception {

    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_REQUEST.getType();
    }
}