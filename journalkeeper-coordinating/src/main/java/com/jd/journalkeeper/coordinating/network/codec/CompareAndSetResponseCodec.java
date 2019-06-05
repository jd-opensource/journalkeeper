package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.CompareAndSetResponse;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * CompareAndSetResponseCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public class CompareAndSetResponseCodec implements CoordinatingPayloadCodec<CompareAndSetResponse> {

    @Override
    public CompareAndSetResponse decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        boolean success = CodecSupport.decodeBoolean(buffer);
        byte[] value = CodecSupport.decodeBytes(buffer);

        CompareAndSetResponse compareAndSetResponse = new CompareAndSetResponse();
        compareAndSetResponse.setSuccess(success);
        compareAndSetResponse.setValue(value);
        return compareAndSetResponse;
    }

    @Override
    public void encode(CompareAndSetResponse payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBoolean(buffer, payload.isSuccess());
        CodecSupport.encodeBytes(buffer, payload.getValue());
    }

    @Override
    public int type() {
        return CoordinatingCommands.COMPARE_AND_SET_RESPONSE.getType();
    }
}