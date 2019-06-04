package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.PutResponse;
import io.netty.buffer.ByteBuf;

/**
 * PutResponseCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class PutResponseCodec implements CoordinatingPayloadCodec<PutResponse> {

    @Override
    public PutResponse decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        return null;
    }

    @Override
    public void encode(PutResponse payload, ByteBuf buffer) throws Exception {

    }

    @Override
    public int type() {
        return CoordinatingCommands.PUT_RESPONSE.getType();
    }
}