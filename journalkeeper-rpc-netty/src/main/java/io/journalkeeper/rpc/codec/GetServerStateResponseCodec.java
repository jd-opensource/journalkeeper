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
import io.journalkeeper.rpc.server.GetServerStateResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class GetServerStateResponseCodec extends ResponseCodec<GetServerStateResponse> implements Type {

    @Override
    public int type() {
        return RpcTypes.GET_SERVER_STATE_RESPONSE;
    }

    @Override
    protected void encodeResponse(JournalKeeperHeader header, GetServerStateResponse response, ByteBuf buffer) throws Exception {
        //long lastIncludedIndex, int lastIncludedTerm, long offset, byte[] data, boolean done
        CodecSupport.encodeLong(buffer, response.getLastIncludedIndex());
        CodecSupport.encodeInt(buffer, response.getLastIncludedTerm());
        CodecSupport.encodeLong(buffer, response.getOffset());
        CodecSupport.encodeBytes(buffer, response.getData());
        CodecSupport.encodeBoolean(buffer, response.isDone());
        CodecSupport.encodeInt(buffer, response.getIteratorId());

    }

    @Override
    protected GetServerStateResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new GetServerStateResponse(
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeBytes(buffer),
                CodecSupport.decodeBoolean(buffer),
                CodecSupport.decodeInt(buffer)
        );
    }
}
