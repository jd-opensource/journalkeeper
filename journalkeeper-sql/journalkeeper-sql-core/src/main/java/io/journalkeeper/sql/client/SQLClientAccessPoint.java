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
package io.journalkeeper.sql.client;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.client.Client;
import io.journalkeeper.rpc.RpcAccessPointFactory;
import io.journalkeeper.rpc.client.ClientServerRpcAccessPoint;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.Response;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.serializer.KryoSerializer;
import io.journalkeeper.utils.spi.ServiceSupport;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * SQLClientAccessPoint
 * author: gaohaoxiang
 *
 * date: 2019/6/10
 */
public class SQLClientAccessPoint {

    private Properties config;
    private Serializer<WriteRequest> entrySerializer;
    private Serializer<ReadRequest> querySerializer;
    private Serializer<Response> resultSerializer;

    public SQLClientAccessPoint(Properties config) {
        this(config,
                new KryoSerializer<>(WriteRequest.class),
                new KryoSerializer<>(ReadRequest.class),
                new KryoSerializer<>(Response.class));
    }

    public SQLClientAccessPoint(Properties config,
                                Serializer<WriteRequest> entrySerializer,
                                Serializer<ReadRequest> querySerializer,
                                Serializer<Response> resultSerializer) {
        this.config = config;
        this.entrySerializer = entrySerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
    }

    public SQLClient createClient(List<URI> servers) {
        RpcAccessPointFactory rpcAccessPoint = ServiceSupport.load(RpcAccessPointFactory.class);
        ClientServerRpcAccessPoint clientServerRpcAccessPoint = rpcAccessPoint.createClientServerRpcAccessPoint(servers, config);
        Client<WriteRequest, ReadRequest, Response> client = new Client<>(clientServerRpcAccessPoint, entrySerializer, querySerializer, resultSerializer, config);
        return new SQLClient(servers, config, client);
    }
}