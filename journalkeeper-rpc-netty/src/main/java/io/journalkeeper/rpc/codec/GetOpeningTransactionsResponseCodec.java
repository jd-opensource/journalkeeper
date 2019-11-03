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

import io.journalkeeper.rpc.client.CreateTransactionResponse;
import io.journalkeeper.rpc.client.GetOpeningTransactionsResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.codec.Decoder;
import io.journalkeeper.rpc.remoting.transport.codec.Encoder;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.netty.buffer.ByteBuf;

import java.util.UUID;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class GetOpeningTransactionsResponseCodec extends LeaderResponseCodec<GetOpeningTransactionsResponse> implements Type {
    @Override
    protected void encodeLeaderResponse(GetOpeningTransactionsResponse leaderResponse, ByteBuf buffer) throws Exception {
        CodecSupport.encodeCollection(buffer, leaderResponse.getTransactionIds(),
                (obj, buffer1) -> CodecSupport.encodeUUID(buffer1, (UUID) obj));
    }

    @Override
    protected GetOpeningTransactionsResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new GetOpeningTransactionsResponse(CodecSupport.decodeCollection(buffer, CodecSupport::decodeUUID));
    }

    @Override
    public int type() {
        return RpcTypes.GET_OPENING_TRANSACTIONS_RESPONSE;
    }
}
