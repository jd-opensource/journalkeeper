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
import io.journalkeeper.base.VoidSerializer;
import io.journalkeeper.coordinating.state.domain.StateReadRequest;
import io.journalkeeper.coordinating.state.domain.StateResponse;
import io.journalkeeper.coordinating.state.domain.StateWriteRequest;
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
    private Serializer<StateWriteRequest> entrySerializer;
    private Serializer<Void> entryResultSerializer;
    private Serializer<StateReadRequest> querySerializer;
    private Serializer<StateResponse> resultSerializer;

    public CoordinatingClientAccessPoint(Properties config) {
        this(config,
                new KryoSerializer<>(StateWriteRequest.class),
                new VoidSerializer(),
                new KryoSerializer<>(StateReadRequest.class),
                new KryoSerializer<>(StateResponse.class));
    }

    public CoordinatingClientAccessPoint(Properties config,
                                         Serializer<StateWriteRequest> entrySerializer,
                                         Serializer<Void> entryResultSerializer,
                                         Serializer<StateReadRequest> querySerializer,
                                         Serializer<StateResponse> resultSerializer) {
        this.config = config;
        this.entrySerializer = entrySerializer;
        this.entryResultSerializer = entryResultSerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
    }

    public CoordinatingClient createClient(List<URI> servers) {
        RpcAccessPointFactory rpcAccessPoint = ServiceSupport.load(RpcAccessPointFactory.class);
        ClientServerRpcAccessPoint clientServerRpcAccessPoint = rpcAccessPoint.createClientServerRpcAccessPoint(servers, config);
        Client<StateWriteRequest, Void, StateReadRequest, StateResponse> client = new Client<>(clientServerRpcAccessPoint, entrySerializer, entryResultSerializer, querySerializer, resultSerializer, config);
        return new CoordinatingClient(servers, config, client);
    }
}