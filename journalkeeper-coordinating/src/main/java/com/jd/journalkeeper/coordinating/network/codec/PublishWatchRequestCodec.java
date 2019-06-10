package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.coordinating.network.CoordinatingCommands;
import com.jd.journalkeeper.coordinating.network.command.PublishWatchRequest;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.netty.buffer.ByteBuf;

/**
 * PublishWatchRequestCodec
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/10
 */
public class PublishWatchRequestCodec implements CoordinatingPayloadCodec<PublishWatchRequest> {

    @Override
    public Object decode(CoordinatingHeader header, ByteBuf buffer) throws Exception {
        int type = CodecSupport.decodeInt(buffer);
        byte[] key = CodecSupport.decodeBytes(buffer);
        byte[] value = CodecSupport.decodeBytes(buffer);
        long modifyTime = CodecSupport.decodeLong(buffer);
        long createTime = CodecSupport.decodeLong(buffer);

        PublishWatchRequest publishWatchRequest = new PublishWatchRequest();
        publishWatchRequest.setType(type);
        publishWatchRequest.setKey(key);
        publishWatchRequest.setValue(value);
        publishWatchRequest.setModifyTime(modifyTime);
        publishWatchRequest.setCreateTime(createTime);
        return publishWatchRequest;
    }

    @Override
    public void encode(PublishWatchRequest payload, ByteBuf buffer) throws Exception {
        CodecSupport.encodeInt(buffer, payload.getType());
        CodecSupport.encodeBytes(buffer, payload.getKey());
        CodecSupport.encodeBytes(buffer, payload.getValue());
        CodecSupport.encodeLong(buffer, payload.getModifyTime());
        CodecSupport.encodeLong(buffer, payload.getCreateTime());
    }

    @Override
    public int type() {
        return CoordinatingCommands.PUBLISH_WATCH_REQUEST.getType();
    }
}