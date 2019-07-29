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

import com.jd.journalkeeper.core.api.ResponseConfig;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateRequest;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-03-29
 */
public class UpdateClusterStateRequestCodec extends GenericPayloadCodec<UpdateClusterStateRequest> implements Type {
    @Override
    protected void encodePayload(UpdateClusterStateRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeBytes(buffer, request.getEntry());
        CodecSupport.encodeShort(buffer, (short )request.getPartition());
        CodecSupport.encodeShort(buffer, (short )request.getBatchSize());
        CodecSupport.encodeByte(buffer, (byte) request.getResponseConfig().value());
    }

    @Override
    protected UpdateClusterStateRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new UpdateClusterStateRequest(
                CodecSupport.decodeBytes(buffer),
                CodecSupport.decodeShort(buffer),
                CodecSupport.decodeShort(buffer),
                ResponseConfig.valueOf(CodecSupport.decodeByte(buffer)));
    }

    @Override
    public int type() {
        return RpcTypes.UPDATE_CLUSTER_STATE_REQUEST;
    }
}
