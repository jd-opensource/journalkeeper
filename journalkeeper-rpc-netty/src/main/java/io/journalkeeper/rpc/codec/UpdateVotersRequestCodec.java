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
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateVotersRequest;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.codec.Encoder;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.journalkeeper.rpc.remoting.transport.exception.TransportException;
import io.netty.buffer.ByteBuf;

import java.net.URI;

import static com.sun.org.apache.xml.internal.resolver.Catalog.URI;

/**
 * @author LiYue
 * Date: 2019-03-29
 */
public class UpdateVotersRequestCodec extends GenericPayloadCodec<UpdateVotersRequest> implements Type {
    @Override
    protected void encodePayload(UpdateVotersRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeList(buffer, request.getOldConfig(),  (obj, buffer1) -> CodecSupport.encodeUri(buffer1, (URI) obj));
        CodecSupport.encodeList(buffer, request.getNewConfig(),  (obj, buffer1) -> CodecSupport.encodeUri(buffer1, (URI) obj));

    }

    @Override
    protected UpdateVotersRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new UpdateVotersRequest(
                CodecSupport.decodeList(buffer, CodecSupport::decodeUri),
                CodecSupport.decodeList(buffer, CodecSupport::decodeUri)
                );
    }

    @Override
    public int type() {
        return RpcTypes.UPDATE_VOTERS_REQUEST;
    }
}
