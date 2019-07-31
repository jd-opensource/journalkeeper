/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.remoting.example;


import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.codec.Codec;
import io.journalkeeper.rpc.remoting.transport.command.Direction;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.netty.buffer.ByteBuf;

/**
 * jmq协议头编解码器
 * author: gaohaoxiang
 *
 * date: 2018/8/21
 */
public class TestHeaderCodec implements Codec {

    // MAGIC + VERSION + FLAG + REQUESTID + COMMANDTYPE + SENDTIME + STATUS + ERRORMSG
    public static final int HEADER_LENGTH = 4 + 1 + 1 + 4 + 1 + 8;

    @Override
    public TestHeader decode(ByteBuf buffer) throws TransportException.CodecException {
        if (buffer.readableBytes() < HEADER_LENGTH) {
            return null;
        }

        int magic = buffer.readInt();
        if (magic != TestHeader.MAGIC) {
            return null;
        }

        byte version = buffer.readByte();
        byte identity = buffer.readByte();
        int requestId = buffer.readInt();
        byte type = buffer.readByte();
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

        return new TestHeader(version, oneWay, direction, requestId, type, sendTime, status, error);
    }

    @Override
    public void encode(Object payload, ByteBuf buffer) throws TransportException.CodecException {
        TestHeader header = (TestHeader) payload;
        // 响应类型
        byte identity = (byte) ((header.getDirection().ordinal() & 0x1) | (header.isOneWay() ? 0x2: 0x0));

        buffer.writeInt(TestHeader.MAGIC);
        buffer.writeByte(header.getVersion());
        buffer.writeByte(identity);
        buffer.writeInt(header.getRequestId());
        buffer.writeByte(header.getType());
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