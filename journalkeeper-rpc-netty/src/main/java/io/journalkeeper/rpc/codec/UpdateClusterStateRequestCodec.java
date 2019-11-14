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
package io.journalkeeper.rpc.codec;

import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.SerializedUpdateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-03-29
 */
public class UpdateClusterStateRequestCodec extends GenericPayloadCodec<UpdateClusterStateRequest> implements Type {
    @Override
    protected void encodePayload(UpdateClusterStateRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeUUID(buffer, request.getTransactionId());
        CodecSupport.encodeList(buffer, request.getRequests(), (obj, buffer1) -> {
            SerializedUpdateRequest request1  = (SerializedUpdateRequest) obj;
            CodecSupport.encodeBytes(buffer1, request1.getEntry());
            CodecSupport.encodeShort(buffer1, (short )request1.getPartition());
            CodecSupport.encodeShort(buffer1, (short )request1.getBatchSize());

        });
        CodecSupport.encodeBoolean(buffer, request.isIncludeHeader());
        CodecSupport.encodeByte(buffer, (byte) request.getResponseConfig().value());

    }

    @Override
    protected UpdateClusterStateRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) {
        return new UpdateClusterStateRequest(
                CodecSupport.decodeUUID(buffer),
                CodecSupport.decodeList(buffer, buffer1 -> new SerializedUpdateRequest(
                        CodecSupport.decodeBytes(buffer1),
                        CodecSupport.decodeShort(buffer1),
                        CodecSupport.decodeShort(buffer1)
                )),
                CodecSupport.decodeBoolean(buffer),
                ResponseConfig.valueOf(CodecSupport.decodeByte(buffer)));
    }

    @Override
    public int type() {
        return RpcTypes.UPDATE_CLUSTER_STATE_REQUEST;
    }
}
