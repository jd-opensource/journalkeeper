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

import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.journalkeeper.rpc.server.GetServerEntriesResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class GetServerEntriesResponseCodec extends ResponseCodec<GetServerEntriesResponse> implements Type {
    @Override
    protected void encodeResponse(JournalKeeperHeader header, GetServerEntriesResponse response, ByteBuf buffer) throws Exception {
        // List<byte []> entries, long minIndex, long lastApplied
        CodecSupport.encodeList(buffer, response.getEntries(),
                (obj, buffer1) -> CodecSupport.encodeBytes(buffer1, (byte[]) obj));
        CodecSupport.encodeLong(buffer, response.getMinIndex());
        CodecSupport.encodeLong(buffer, response.getLastApplied());
    }

    @Override
    protected GetServerEntriesResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new GetServerEntriesResponse(
                CodecSupport.decodeList(buffer, CodecSupport::decodeBytes),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.GET_SERVER_ENTRIES_RESPONSE;
    }
}
