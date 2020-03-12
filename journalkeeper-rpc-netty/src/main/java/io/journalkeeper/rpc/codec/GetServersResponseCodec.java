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

import io.journalkeeper.core.api.ClusterConfiguration;
import io.journalkeeper.rpc.client.GetServersResponse;
import io.journalkeeper.rpc.header.JournalKeeperHeader;
import io.journalkeeper.rpc.remoting.serialize.CodecSupport;
import io.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

import java.net.URI;
import java.util.List;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class GetServersResponseCodec extends ResponseCodec<GetServersResponse> implements Type {
    @Override
    protected void encodeResponse(JournalKeeperHeader header, GetServersResponse response, ByteBuf buffer) throws Exception {
        ClusterConfiguration clusterConfiguration = response == null ? new ClusterConfiguration() : response.getClusterConfiguration();
        if (null == clusterConfiguration) clusterConfiguration = new ClusterConfiguration();

        CodecSupport.encodeString(buffer, uriToString(clusterConfiguration.getLeader()));
        CodecSupport.encodeList(buffer, clusterConfiguration.getVoters(), (obj, buffer1) -> CodecSupport.encodeUri(buffer1, (URI) obj));
        CodecSupport.encodeList(buffer, clusterConfiguration.getObservers(), (obj, buffer1) -> CodecSupport.encodeUri(buffer1, (URI) obj));
    }

    @Override
    protected GetServersResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        URI leader = CodecSupport.decodeUri(buffer);
        List<URI> voters = CodecSupport.decodeList(buffer, CodecSupport::decodeUri);
        List<URI> observers = CodecSupport.decodeList(buffer, CodecSupport::decodeUri);

        return new GetServersResponse(new ClusterConfiguration(leader, voters, observers));
    }

    @Override
    public int type() {
        return RpcTypes.GET_SERVERS_RESPONSE;
    }

    private String uriToString(URI uri) {
        return null == uri ? null : uri.toString();
    }
}
