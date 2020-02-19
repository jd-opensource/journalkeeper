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
package io.journalkeeper.coordinating.client;

import io.journalkeeper.coordinating.state.domain.ReadRequest;
import io.journalkeeper.coordinating.state.domain.ReadResponse;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.coordinating.state.domain.WriteResponse;
import io.journalkeeper.core.serialize.WrappedBootStrap;
import io.journalkeeper.core.serialize.WrappedRaftClient;

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

    public CoordinatingClientAccessPoint(Properties config) {
        this.config = config;
    }

    public CoordinatingClient createClient(List<URI> servers) {
        WrappedBootStrap<WriteRequest, WriteResponse, ReadRequest, ReadResponse> bootStrap =
                new WrappedBootStrap<>(servers, config);
        WrappedRaftClient<WriteRequest, WriteResponse, ReadRequest, ReadResponse> client =
                bootStrap.getClient();
        return new CoordinatingClient(servers, config, client);
    }
}