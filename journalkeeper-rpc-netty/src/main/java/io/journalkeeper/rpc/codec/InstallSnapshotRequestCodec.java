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
import io.journalkeeper.rpc.server.InstallSnapshotRequest;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class InstallSnapshotRequestCodec extends GenericPayloadCodec<InstallSnapshotRequest> implements Type {
    @Override
    protected void encodePayload(JournalKeeperHeader header, InstallSnapshotRequest request, ByteBuf buffer) throws Exception {
        CodecSupport.encodeInt(buffer, request.getTerm());
        CodecSupport.encodeUri(buffer, request.getLeaderId());
        CodecSupport.encodeLong(buffer, request.getLastIncludedIndex());
        CodecSupport.encodeInt(buffer, request.getLastIncludedTerm());
        CodecSupport.encodeInt(buffer, request.getOffset());
        CodecSupport.encodeBytes(buffer, request.getData());
        CodecSupport.encodeBoolean(buffer, request.isDone());
    }

    @Override
    protected InstallSnapshotRequest decodePayload(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new InstallSnapshotRequest(
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeUri(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeInt(buffer),
                CodecSupport.decodeBytes(buffer),
                CodecSupport.decodeBoolean(buffer));
    }

    @Override
    public int type() {
        return RpcTypes.INSTALL_SNAPSHOT_REQUEST;
    }
}
