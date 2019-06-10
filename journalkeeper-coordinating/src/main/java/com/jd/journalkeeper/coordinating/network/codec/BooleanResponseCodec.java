package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.BooleanResponse;
import io.netty.buffer.ByteBuf;

/**
 * BooleanResponseCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class BooleanResponseCodec implements CoordinatingPayloadCodec<BooleanResponse> {

    @Override
    public BooleanResponse decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        return new BooleanResponse();
    }

    @Override
    public void encode(BooleanResponse payload, ByteBuf buffer) throws Exception {

    }

    @Override
    public int type() {
        return CoordinatingCommands.BOOLEAN_RESPONSE.getType();
    }
}