package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.GetResponse;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
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
        byte[] value = CodecSupport.decodeBytes(buffer);
        long modifyTime = CodecSupport.decodeLong(buffer);
        long createTime = CodecSupport.decodeLong(buffer);

        GetResponse getResponse = new GetResponse();
        getResponse.setValue(value);
        getResponse.setModifyTime(modifyTime);
        getResponse.setCreateTime(createTime);
        return getResponse;
    }

    @Override
    public void encode(GetResponse payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, payload.getValue());
        CodecSupport.encodeLong(buffer, payload.getModifyTime());
        CodecSupport.encodeLong(buffer, payload.getCreateTime());
    }

    @Override
    public int type() {
        return CoordinatingCommands.GET_RESPONSE.getType();
    }
}