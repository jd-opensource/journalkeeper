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

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.rpc.client.CompleteTransactionRequest;
import io.journalkeeper.rpc.client.ConvertRollRequest;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class CompleteTransactionRequestCodec extends GenericPayloadCodec<CompleteTransactionRequest> implements Type {
    @Override
    protected void encodePayload(CompleteTransactionRequest request, ByteBuf buffer) throws Exception {

        CodecSupport.encodeUUID(buffer, request.getTransactionId());
        CodecSupport.encodeBoolean(buffer, request.isCommitOrAbort());
    }

    @Override
    protected CompleteTransactionRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new CompleteTransactionRequest(
                CodecSupport.decodeUUID(buffer),
                CodecSupport.decodeBoolean(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.COMPLETE_TRANSACTION_REQUEST;
    }
}