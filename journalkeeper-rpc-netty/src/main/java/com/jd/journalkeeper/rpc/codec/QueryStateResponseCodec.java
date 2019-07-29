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

import com.jd.journalkeeper.rpc.client.QueryStateResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Types;
import io.netty.buffer.ByteBuf;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class QueryStateResponseCodec extends LeaderResponseCodec<QueryStateResponse> implements Types {
    @Override
    protected void encodeLeaderResponse(QueryStateResponse response, ByteBuf buffer) throws Exception {
        CodecSupport.encodeLong(buffer, response.getLastApplied());
        CodecSupport.encodeBytes(buffer, response.getResult());
    }

    @Override
    protected QueryStateResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        long lastApplied = CodecSupport.decodeLong(buffer);
        byte [] result = CodecSupport.decodeBytes(buffer);
        return new QueryStateResponse(result, lastApplied);
    }


    @Override
    public int[] types() {
        return new int[] {
                RpcTypes.QUERY_CLUSTER_STATE_RESPONSE,
                RpcTypes.QUERY_SERVER_STATE_RESPONSE,
                RpcTypes.QUERY_SNAPSHOT_RESPONSE
        };
    }
}
