package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.PutResponse;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
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
        long modifyTime = CodecSupport.decodeLong(buffer);
        long createTime = CodecSupport.decodeLong(buffer);

        PutResponse putResponse = new PutResponse();
        putResponse.setModifyTime(modifyTime);
        putResponse.setCreateTime(createTime);
        return putResponse;
    }

    @Override
    public void encode(PutResponse payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, payload.getModifyTime());
        CodecSupport.encodeLong(buffer, payload.getCreateTime());
    }

    @Override
    public int type() {
        return CoordinatingCommands.PUT_RESPONSE.getType();
    }
}