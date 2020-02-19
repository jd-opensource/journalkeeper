/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.remoting.transport.codec;

import io.journalkeeper.rpc.codec.RpcTypes;
import io.journalkeeper.rpc.remoting.transport.command.Command;
import io.journalkeeper.rpc.remoting.transport.command.Header;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * jmq解码
 * author: gaohaoxiang
 *
 * date: 2018/8/21
 */
public class DefaultDecoder implements Decoder {

    protected static final Logger logger = LoggerFactory.getLogger(DefaultDecoder.class);
    // LENGTH
    private static final int LENGTH_FIELD_LENGTH = 4;
    private Codec headerCodec;
    private PayloadCodecFactory payloadCodecFactory;

    public DefaultDecoder(Codec headerCodec, PayloadCodecFactory payloadCodecFactory) {
        this.headerCodec = headerCodec;
        this.payloadCodecFactory = payloadCodecFactory;
    }

    @Override
    public Object decode(ByteBuf buffer) throws TransportException.CodecException {
        try {
            if (!buffer.isReadable(LENGTH_FIELD_LENGTH)) {
                return null;
            }
            buffer.markReaderIndex();
            int length = readLength(buffer);
            if (buffer.readableBytes() < length) {
                buffer.resetReaderIndex();
                return null;
            }
            return doDecode(buffer);
        } catch (Exception e) {
            logger.error("decode exception", e);
            throw new TransportException.CodecException(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public Object doDecode(ByteBuf buffer) throws Exception {
        Header header = (Header) headerCodec.decode(buffer);
        if (header == null) {
            return null;
        }
        Object payload = null;
        if (header.getType() != RpcTypes.VOID_PAYLOAD) {

            PayloadDecoder decoder = payloadCodecFactory.getDecoder(header);
            if (decoder == null) {
                throw new TransportException.CodecException(String.format("unsupported decode payload type,header: %s", header));
            }
            payload = decoder.decode(header, buffer);
        }
        return new Command(header, payload);

    }

    protected int readLength(ByteBuf buffer) {
        return buffer.readInt() - 4;
    }
}