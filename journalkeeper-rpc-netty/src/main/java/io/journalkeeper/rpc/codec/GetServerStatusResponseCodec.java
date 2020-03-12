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

import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.ServerStatus;
import io.journalkeeper.core.api.VoterState;
import io.journalkeeper.rpc.client.GetServerStatusResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

/**
 * @author LiYue
 * Date: 2019-09-04
 */
public class GetServerStatusResponseCodec extends ResponseCodec<GetServerStatusResponse> implements Type {
    @Override
    protected void encodeResponse(JournalKeeperHeader header, GetServerStatusResponse response, ByteBuf buffer) throws Exception {
        ServerStatus serverStatus = response.getServerStatus();
        if (null == serverStatus) serverStatus = new ServerStatus();
        CodecSupport.encodeString(buffer, serverStatus.getRoll() == null ? "" : serverStatus.getRoll().name());
        CodecSupport.encodeString(buffer, serverStatus.getVoterState() == null ? "" : serverStatus.getVoterState().name());

        CodecSupport.encodeLong(buffer, serverStatus.getMinIndex());
        CodecSupport.encodeLong(buffer, serverStatus.getMaxIndex());
        CodecSupport.encodeLong(buffer, serverStatus.getCommitIndex());
        CodecSupport.encodeLong(buffer, serverStatus.getLastApplied());
    }

    @Override
    protected GetServerStatusResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        String str = CodecSupport.decodeString(buffer);
        RaftServer.Roll roll = str.isEmpty() ? null : RaftServer.Roll.valueOf(str);
        str = CodecSupport.decodeString(buffer);
        VoterState voterState = str.isEmpty() ? null : VoterState.valueOf(str);

        return new GetServerStatusResponse(new ServerStatus(
                roll,
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer),
                CodecSupport.decodeLong(buffer),
                voterState
        ));
    }

    @Override
    public int type() {
        return RpcTypes.GET_SERVER_STATUS_RESPONSE;
    }
}
