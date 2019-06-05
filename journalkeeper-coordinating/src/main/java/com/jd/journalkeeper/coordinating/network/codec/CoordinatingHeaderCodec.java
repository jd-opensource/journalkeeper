package com.jd.journalkeeper.coordinating.network.codec;

import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Codec;
import com.jd.journalkeeper.rpc.remoting.transport.command.Direction;
import com.jd.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.netty.buffer.ByteBuf;

public class CoordinatingHeaderCodec implements Codec {

    private static final int HEADER_LENGTH = 4 + 1 + 1 + 4 + 4 + 8;

    @Override
    public CoordinatingHeader decode(ByteBuf buffer) throws TransportException.CodecException {
        if (buffer.readableBytes() < HEADER_LENGTH) {
            return null;
        }

        int magic = buffer.readInt();
        if (magic != CoordinatingHeader.MAGIC) {
            return null;
        }

        byte version = buffer.readByte();
        byte identity = buffer.readByte();
        int requestId = buffer.readInt();
        int type = buffer.readInt();
        long sendTime = buffer.readLong();
        short status = 0;
        String error = null;
        Direction direction = Direction.valueOf(identity & 0x1);
        boolean oneWay = ((identity >> 1) & 0x1) == 0x1;

        if (direction.equals(Direction.RESPONSE)) {
            // 1个字节的状态码
            status = buffer.readUnsignedByte();
            // 2个字节的异常长度
            // 异常信息
            try {
                error = CodecSupport.decodeString(buffer);
            } catch (Exception e) {
                throw new TransportException.CodecException(e.getMessage());
            }
        }

        return new CoordinatingHeader(version, oneWay, direction, requestId, type, sendTime, status, error);
    }

    @Override
    public void encode(Object payload, ByteBuf buffer) throws TransportException.CodecException {
        CoordinatingHeader header = (CoordinatingHeader) payload;
        // 响应类型
        byte identity = (byte) ((header.getDirection().ordinal() & 0x1) | (header.isOneWay() ? 0x2: 0x0));

        buffer.writeInt(CoordinatingHeader.MAGIC);
        buffer.writeByte(header.getVersion());
        buffer.writeByte(identity);
        buffer.writeInt(header.getRequestId());
        buffer.writeInt(header.getType());
        buffer.writeLong(header.getSendTime());
        if (header.getDirection().equals(Direction.RESPONSE)) {
            buffer.writeByte(header.getStatus());
            try {
                CodecSupport.encodeString(buffer, header.getError());
            } catch (Exception e) {
                throw new TransportException.CodecException(e.getMessage());
            }
        }
    }
}