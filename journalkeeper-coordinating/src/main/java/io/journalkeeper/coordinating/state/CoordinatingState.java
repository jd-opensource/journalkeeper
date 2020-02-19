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
package io.journalkeeper.coordinating.state;

import io.journalkeeper.coordinating.state.config.CoordinatingConfigs;
import io.journalkeeper.coordinating.state.domain.ReadRequest;
import io.journalkeeper.coordinating.state.domain.ReadResponse;
import io.journalkeeper.coordinating.state.domain.StateCodes;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.coordinating.state.domain.WriteResponse;
import io.journalkeeper.coordinating.state.store.KVStore;
import io.journalkeeper.coordinating.state.store.KVStoreManager;
import io.journalkeeper.core.serialize.WrappedState;
import io.journalkeeper.core.serialize.WrappedStateResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * CoordinatingState
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class CoordinatingState implements WrappedState<WriteRequest, WriteResponse, ReadRequest, ReadResponse> {

    private Properties properties;
    private KVStore kvStore;
    private CoordinatingStateHandler handler;

    @Override
    public void recover(Path path, Properties properties) {
        this.properties = properties;
        this.kvStore = KVStoreManager.getFactory(properties.getProperty(CoordinatingConfigs.STATE_STORE)).create(path, properties);
        this.handler = new CoordinatingStateHandler(properties, kvStore);
    }

    @Override
    public WrappedStateResult<WriteResponse> executeAndNotify(WriteRequest request) {
        WriteResponse response = execute(request);
        if (response.getCode() != StateCodes.SUCCESS.getCode()) {
            return new WrappedStateResult<>(response, null);
        }
        Map<String, String> events = new HashMap<>(3);
        events.put("type", String.valueOf(request.getType()));
        events.put("key", new String(request.getKey(), StandardCharsets.UTF_8));
        if (request.getValue() != null) {
            events.put("value", new String(request.getValue(), StandardCharsets.UTF_8));
        }
        return new WrappedStateResult<>(response, events);
    }

    @Override
    public WriteResponse execute(WriteRequest request) {
        return handler.handle(request);
    }

    @Override
    public ReadResponse query(ReadRequest request) {
        return handler.handle(request);
    }

    @Override
    public void close() {
        kvStore.close();
    }
}
