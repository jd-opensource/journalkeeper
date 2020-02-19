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
package io.journalkeeper.coordinating.server;

import io.journalkeeper.coordinating.state.CoordinatorStateFactory;
import io.journalkeeper.coordinating.state.domain.ReadRequest;
import io.journalkeeper.coordinating.state.domain.ReadResponse;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.coordinating.state.domain.WriteResponse;
import io.journalkeeper.core.api.RaftServer;
import io.journalkeeper.core.serialize.WrappedStateFactory;

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
    private WrappedStateFactory<WriteRequest, WriteResponse, ReadRequest, ReadResponse> stateFactory;

    public CoordinatingServerAccessPoint(Properties config) {
        this(config,
                new CoordinatorStateFactory());
    }


    public CoordinatingServerAccessPoint(Properties config,
                                         WrappedStateFactory<WriteRequest, WriteResponse, ReadRequest, ReadResponse> stateFactory) {
        this.config = config;
        this.stateFactory = stateFactory;
    }

    public CoordinatingServer createServer(URI current, List<URI> servers, RaftServer.Roll role) {
        return new CoordinatingServer(current, servers, config, role, stateFactory);
    }
}