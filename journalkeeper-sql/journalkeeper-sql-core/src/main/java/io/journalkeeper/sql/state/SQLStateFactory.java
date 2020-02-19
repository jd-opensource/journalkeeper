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
package io.journalkeeper.sql.state;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.sql.client.domain.ReadRequest;
import io.journalkeeper.sql.client.domain.ReadResponse;
import io.journalkeeper.sql.client.domain.WriteRequest;
import io.journalkeeper.sql.client.domain.WriteResponse;

/**
 * SQLStateFactory
 * author: gaohaoxiang
 * date: 2019/8/2
 */
public class SQLStateFactory implements StateFactory {
    private final Serializer<WriteRequest> writeRequestSerializer;
    private final Serializer<WriteResponse> writeResponseSerializer;
    private final Serializer<ReadRequest> readRequestSerializer;
    private final Serializer<ReadResponse> readResponseSerializer;

    public SQLStateFactory(Serializer<WriteRequest> writeRequestSerializer, Serializer<WriteResponse> writeResponseSerializer, Serializer<ReadRequest> readRequestSerializer, Serializer<ReadResponse> readResponseSerializer) {
        this.writeRequestSerializer = writeRequestSerializer;
        this.writeResponseSerializer = writeResponseSerializer;
        this.readRequestSerializer = readRequestSerializer;
        this.readResponseSerializer = readResponseSerializer;
    }

    @Override
    public State createState() {
        return new SQLState(writeRequestSerializer, writeResponseSerializer, readRequestSerializer, readResponseSerializer);
    }

    public Serializer<WriteRequest> getWriteRequestSerializer() {
        return writeRequestSerializer;
    }

    public Serializer<WriteResponse> getWriteResponseSerializer() {
        return writeResponseSerializer;
    }

    public Serializer<ReadRequest> getReadRequestSerializer() {
        return readRequestSerializer;
    }

    public Serializer<ReadResponse> getReadResponseSerializer() {
        return readResponseSerializer;
    }
}