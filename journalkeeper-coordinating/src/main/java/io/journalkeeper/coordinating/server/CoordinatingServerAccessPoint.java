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
package io.journalkeeper.coordinating.server;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.coordinating.state.CoordinatorStateFactory;
import io.journalkeeper.coordinating.state.domain.StateReadRequest;
import io.journalkeeper.coordinating.state.domain.StateResponse;
import io.journalkeeper.coordinating.state.domain.StateWriteRequest;
import io.journalkeeper.coordinating.state.serializer.KryoSerializer;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.api.StateFactory;

import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * CoordinatingServerAccessPoint
 * author: gaohaoxiang
 *
 * date: 2019/6/10
 */
public class CoordinatingServerAccessPoint {

    private Properties config;
    private StateFactory<StateWriteRequest, StateReadRequest, StateResponse> stateFactory;
    private Serializer<StateWriteRequest> entrySerializer;
    private Serializer<StateReadRequest> querySerializer;
    private Serializer<StateResponse> resultSerializer;

    public CoordinatingServerAccessPoint(Properties config) {
        this(config,
                new CoordinatorStateFactory(),
                new KryoSerializer<>(StateWriteRequest.class),
                new KryoSerializer<>(StateReadRequest.class),
                new KryoSerializer<>(StateResponse.class));
    }

    public CoordinatingServerAccessPoint(Properties config,
                                         StateFactory<StateWriteRequest, StateReadRequest, StateResponse> stateFactory) {
        this(config,
                stateFactory,
                new KryoSerializer<>(StateWriteRequest.class),
                new KryoSerializer<>(StateReadRequest.class),
                new KryoSerializer<>(StateResponse.class));
    }

    public CoordinatingServerAccessPoint(Properties config,
                                         StateFactory<StateWriteRequest, StateReadRequest, StateResponse> stateFactory,
                                         Serializer<StateWriteRequest> entrySerializer,
                                         Serializer<StateReadRequest> querySerializer,
                                         Serializer<StateResponse> resultSerializer) {
        this.config = config;
        this.stateFactory = stateFactory;
        this.entrySerializer = entrySerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = resultSerializer;
    }

    public CoordinatingServer createServer(URI current, List<URI> servers, RaftServer.Roll role) {
        return new CoordinatingServer(current, servers, config, role, stateFactory, entrySerializer, querySerializer, resultSerializer);
    }
}