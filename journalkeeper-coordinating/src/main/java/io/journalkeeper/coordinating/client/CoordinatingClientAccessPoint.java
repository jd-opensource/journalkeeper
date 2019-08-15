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
package io.journalkeeper.coordinating.client;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.coordinating.state.domain.ReadRequest;
import io.journalkeeper.coordinating.state.domain.ReadResponse;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.coordinating.state.domain.WriteResponse;
import io.journalkeeper.coordinating.state.serializer.KryoSerializer;
import io.journalkeeper.core.client.Client;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.utils.spi.ServiceSupport;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * CoordinatingClientAccessPoint
 * author: gaohaoxiang
 *
 * date: 2019/6/10
 */
public class CoordinatingClientAccessPoint {

    private Properties config;
    private Serializer<WriteRequest> entrySerializer;
    private Serializer<WriteResponse> entryResultSerializer;
    private Serializer<ReadRequest> querySerializer;
    private Serializer<ReadResponse> resultSerializer;

    public CoordinatingClientAccessPoint(Properties config) {
        this(config,
                new KryoSerializer<>(WriteRequest.class),
                new KryoSerializer<>(WriteResponse.class),
                new KryoSerializer<>(ReadRequest.class),
                new KryoSerializer<>(ReadResponse.class));
    }

    public CoordinatingClientAccessPoint(Properties config,
                                         Serializer<WriteRequest> entrySerializer,
                                         Serializer<WriteResponse> entryResultSerializer,
                                         Serializer<ReadRequest> querySerializer,
                                         Serializer<ReadResponse> resultSerializer) {
        this.config = config;
        this.entrySerializer = entrySerializer;
        this.entryResultSerializer = entryResultSerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
    }

    public CoordinatingClient createClient(List<URI> servers) {
        RpcAccessPointFactory rpcAccessPoint = ServiceSupport.load(RpcAccessPointFactory.class);
        ClientServerRpcAccessPoint clientServerRpcAccessPoint = rpcAccessPoint.createClientServerRpcAccessPoint(servers, config);
        Client<WriteRequest, WriteResponse, ReadRequest, ReadResponse> client = new Client<>(clientServerRpcAccessPoint, entrySerializer, entryResultSerializer, querySerializer, resultSerializer, config);
        return new CoordinatingClient(servers, config, client);
    }
}