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
package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.rpc.client.PullEventsRequest;
import com.jd.journalkeeper.rpc.client.RemovePullWatchRequest;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class PullEventsRequestCodec extends GenericPayloadCodec<PullEventsRequest> implements Type {
    @Override
    protected void encodePayload(PullEventsRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, request.getPullWatchId());
        CodecSupport.encodeLong(buffer, request.getAckSequence());

    }

    @Override
    protected PullEventsRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new PullEventsRequest(
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.PULL_EVENTS_REQUEST;
    }
}
