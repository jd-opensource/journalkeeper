package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.ExistResponse;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * ExistResponseCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class ExistResponseCodec implements CoordinatingPayloadCodec<ExistResponse> {

    @Override
    public ExistResponse decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        boolean exist = CodecSupport.decodeBoolean(buffer);

        ExistResponse existResponse = new ExistResponse();
        existResponse.setExist(exist);
        return existResponse;
    }

    @Override
    public void encode(ExistResponse payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBoolean(buffer, payload.isExist());
    }

    @Override
    public int type() {
        return CoordinatingCommands.EXIST_RESPONSE.getType();
    }
}