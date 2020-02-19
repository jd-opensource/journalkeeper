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

import io.journalkeeper.rpc.client.CreateTransactionRequest;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

import static io.journalkeeper.rpc.codec.RpcTypes.CREATE_TRANSACTION_REQUEST;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class CreateTransactionRequestCodec extends GenericPayloadCodec<CreateTransactionRequest> implements Type {
    @Override
    public int type() {
        return CREATE_TRANSACTION_REQUEST;
    }

    @Override
    protected void encodePayload(CreateTransactionRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeMap(buffer, request.getContext(),
                (obj, buffer1) -> CodecSupport.encodeString(buffer1, (String) obj),
                (obj, buffer1) -> CodecSupport.encodeString(buffer1, (String) obj));
    }

    @Override
    protected CreateTransactionRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new CreateTransactionRequest(
                CodecSupport.decodeMap(buffer,
                        CodecSupport::decodeString,
                        CodecSupport::decodeString)
        );
    }
}
