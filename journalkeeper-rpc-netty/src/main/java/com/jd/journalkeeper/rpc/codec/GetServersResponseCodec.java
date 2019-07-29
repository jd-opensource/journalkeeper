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
package com.jd.journalkeeper.rpc.codec;

import com.jd.journalkeeper.core.api.ClusterConfiguration;
import com.jd.journalkeeper.rpc.client.GetServersResponse;
import com.jd.journalkeeper.rpc.header.JournalKeeperHeader;
import com.jd.journalkeeper.rpc.remoting.serialize.CodecSupport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Type;
import io.netty.buffer.ByteBuf;

import java.net.URI;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-04-02
 */
public class GetServersResponseCodec extends ResponseCodec<GetServersResponse> implements Type {
    @Override
    protected void encodeResponse(GetServersResponse response, ByteBuf buffer) throws Exception {
        ClusterConfiguration clusterConfiguration = response == null ? new ClusterConfiguration(): response.getClusterConfiguration();
        if(null == clusterConfiguration) clusterConfiguration = new ClusterConfiguration();

        CodecSupport.encodeString(buffer, uriToString(clusterConfiguration.getLeader()));
        CodecSupport.encodeList(buffer, clusterConfiguration.getVoters(), (obj, buffer1) -> CodecSupport.encodeString(buffer1, uriToString((URI) obj)));
        CodecSupport.encodeList(buffer, clusterConfiguration.getObservers(), (obj, buffer1) -> CodecSupport.encodeString(buffer1, uriToString((URI) obj)));
    }

    // TODO 不存在leader转换uri错误
    @Override
    protected GetServersResponse decodeResponse(JournalKeeperHeader header, ByteBuf buffer) throws Exception {
        URI leader = stringToUri(CodecSupport.decodeString(buffer));
        List<URI> voters = CodecSupport.decodeList(buffer, buffer1 -> stringToUri(CodecSupport.decodeString(buffer1)));
        List<URI> observers = CodecSupport.decodeList(buffer, buffer1 -> stringToUri(CodecSupport.decodeString(buffer1)));

        return new GetServersResponse(new ClusterConfiguration(leader, voters, observers));
    }

    @Override
    public int type() {
        return RpcTypes.GET_SERVERS_RESPONSE;
    }

    private URI stringToUri(String uri) {
        if (uri == null || uri.isEmpty()) {
            return null;
        }
        return URI.create(uri);
    }

    private String uriToString(URI uri) {
        return null == uri ? null : uri.toString();
    }
}
