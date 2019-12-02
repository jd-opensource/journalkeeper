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

import io.journalkeeper.core.api.transaction.UUIDTransactionId;
import io.journalkeeper.rpc.client.CreateTransactionResponse;
import io.journalkeeper.rpc.client.LastAppliedResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class CreateTransactionResponseCodec extends LeaderResponseCodec<CreateTransactionResponse> implements Type {
    @Override
    protected void encodeLeaderResponse(CreateTransactionResponse response, ByteBuf buffer) throws Exception {
        CodecSupport.encodeUUID(buffer, response.getTransactionId().getUuid());
        CodecSupport.encodeLong(buffer, response.getTimestamp());
    }

    @Override
    protected CreateTransactionResponse decodeLeaderResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new CreateTransactionResponse(
                new UUIDTransactionId(CodecSupport.decodeUUID(buffer)),
                CodecSupport.decodeLong(buffer));
    }

    @Override
    public int type() {
        return RpcTypes.CREATE_TRANSACTION_RESPONSE;
    }
}
