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
package io.journalkeeper.rpc.codec;

import io.journalkeeper.rpc.client.PullEventsResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.codec.Decoder;
import io.journalkeeper.rpc.remoting.transport.codec.Encoder;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.journalkeeper.utils.event.PullEvent;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class PullEventsResponseCodec extends ResponseCodec<PullEventsResponse> implements Type {
    @Override
    protected void encodeResponse(JournalKeeperHeader header, PullEventsResponse response, ByteBuf buffer) {
        //boolean success, long journalIndex, int term, int entryCount
        CodecSupport.encodeList(buffer, response.getPullEvents(),
                (obj, buffer1) -> {
                    PullEvent pullEvent = (PullEvent) obj;
                    CodecSupport.encodeInt(buffer1, pullEvent.getEventType());
                    CodecSupport.encodeLong(buffer1, pullEvent.getSequence());
                    CodecSupport.encodeMap(buffer1, pullEvent.getEventData(), new StringCodec(), new StringCodec());

                });
    }

    @Override
    protected PullEventsResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) {
        return new PullEventsResponse(
                CodecSupport.decodeList(buffer, buffer1 -> new PullEvent(
                        CodecSupport.decodeInt(buffer1),
                        CodecSupport.decodeLong(buffer1),
                        CodecSupport.decodeMap(buffer1, new StringCodec(), new StringCodec())
                ))
        );
    }

    @Override
    public int type() {
        return RpcTypes.PULL_EVENTS_RESPONSE;
    }

    private static class StringCodec implements Encoder, Decoder {

        @Override
        public Object decode(ByteBuf buffer) throws TransportException.CodecException {
            return CodecSupport.decodeString(buffer);
        }

        @Override
        public void encode(Object obj, ByteBuf buffer) throws TransportException.CodecException {
            CodecSupport.encodeString(buffer, (String) obj);
        }
    }
}
