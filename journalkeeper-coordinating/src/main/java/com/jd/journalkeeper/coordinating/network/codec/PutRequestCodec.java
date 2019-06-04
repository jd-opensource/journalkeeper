package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.PutRequest;
import io.netty.buffer.ByteBuf;

/**
 * PutRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class PutRequestCodec implements CoordinatingPayloadCodec<PutRequest> {

    @Override
    public PutRequest decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        return null;
    }

    @Override
    public void encode(PutRequest payload, ByteBuf buffer) throws Exception {

    }

    @Override
    public int type() {
        return CoordinatingCommands.PUT_REQUEST.getType();
    }
}