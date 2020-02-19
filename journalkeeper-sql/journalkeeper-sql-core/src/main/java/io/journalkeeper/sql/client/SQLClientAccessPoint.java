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
package io.journalkeeper.sql.client;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.BootStrap;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;
import io.journalkeeper.sql.serializer.KryoSerializer;

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
    private Serializer<WriteRequest> writeRequestSerializer;
    private Serializer<WriteResponse> writeResponseSerializer;
    private Serializer<ReadRequest> readRequestSerializer;
    private Serializer<ReadResponse> readResponseSerializer;

    public SQLClientAccessPoint(Properties config) {
        this(config,
                new KryoSerializer<>(WriteRequest.class),
                new KryoSerializer<>(WriteResponse.class),
                new KryoSerializer<>(ReadRequest.class),
                new KryoSerializer<>(ReadResponse.class));
    }

    public SQLClientAccessPoint(Properties config,
                                Serializer<WriteRequest> writeRequestSerializer,
                                Serializer<WriteResponse> writeResponseSerializer,
                                Serializer<ReadRequest> readRequestSerializer,
                                Serializer<ReadResponse> readResponseSerializer) {
        this.config = config;
        this.writeRequestSerializer = writeRequestSerializer;
        this.writeResponseSerializer = writeResponseSerializer;
        this.readRequestSerializer = readRequestSerializer;
        this.readResponseSerializer = readResponseSerializer;

    }

    public SQLClient createClient(List<URI> servers) {

        BootStrap bootStrap =
                new BootStrap(servers, config);

        return new SQLClient(servers, config, bootStrap, writeRequestSerializer, writeResponseSerializer, readRequestSerializer, readResponseSerializer);
    }
}