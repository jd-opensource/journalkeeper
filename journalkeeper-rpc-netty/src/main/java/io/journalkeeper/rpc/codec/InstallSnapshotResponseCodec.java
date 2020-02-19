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
import io.journalkeeper.rpc.server.InstallSnapshotResponse;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class InstallSnapshotResponseCodec extends ResponseCodec<InstallSnapshotResponse> implements Type {
    @Override
    protected void encodeResponse(InstallSnapshotResponse response, ByteBuf buffer) throws Exception {

        CodecSupport.encodeInt(buffer, response.getTerm());
    }

    @Override
    protected InstallSnapshotResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        return new InstallSnapshotResponse(
                CodecSupport.decodeInt(buffer)
        );
    }

    @Override
    public int type() {
        return RpcTypes.INSTALL_SNAPSHOT_RESPONSE;
    }
}
