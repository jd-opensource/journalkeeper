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

import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Types;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-03-29
 */
public class QueryStateRequestCodec extends GenericPayloadCodec<QueryStateRequest> implements Types {
    @Override
    protected void encodePayload(JournalKeeperHeader header, QueryStateRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, request.getIndex());
        CodecSupport.encodeBytes(buffer, request.getQuery());
    }

    @Override
    protected QueryStateRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        long index = CodecSupport.decodeLong(buffer);
        byte[] query = CodecSupport.decodeBytes(buffer);
        return new QueryStateRequest(query, index);
    }

    @Override
    public int[] types() {
        return new int[]{
                RpcTypes.QUERY_CLUSTER_STATE_REQUEST,
                RpcTypes.QUERY_SERVER_STATE_REQUEST,
                RpcTypes.QUERY_SNAPSHOT_REQUEST
        };
    }
}
