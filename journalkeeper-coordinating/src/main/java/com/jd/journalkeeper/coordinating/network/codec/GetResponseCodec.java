package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.GetResponse;
import io.netty.buffer.ByteBuf;

/**
 * GetResponseCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class GetResponseCodec implements CoordinatingPayloadCodec<GetResponse> {

    @Override
    public GetResponse decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        return null;
    }

    @Override
    public void encode(GetResponse payload, ByteBuf buffer) throws Exception {

    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_RESPONSE.getType();
    }
}