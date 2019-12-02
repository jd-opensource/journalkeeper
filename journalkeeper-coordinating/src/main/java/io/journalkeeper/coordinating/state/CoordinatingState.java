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
package io.journalkeeper.coordinating.state;

import io.journalkeeper.coordinating.state.config.CoordinatingConfigs;
import io.journalkeeper.coordinating.state.domain.ReadRequest;
import io.journalkeeper.coordinating.state.domain.ReadResponse;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.coordinating.state.domain.WriteResponse;
import io.journalkeeper.coordinating.state.store.KVStore;
import io.journalkeeper.coordinating.state.store.KVStoreManager;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.State;
import io.journalkeeper.core.api.StateResult;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;

/**
 * CoordinatingState
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class CoordinatingState implements State<WriteRequest, WriteResponse, ReadRequest, ReadResponse> {

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
    public StateResult<WriteResponse> execute(WriteRequest request, int partition, long index, int batchSize, RaftJournal raftJournal) {
        boolean isSuccess = handler.handle(request);
        if (!isSuccess) {
            return new StateResult<>(null);
        }
        StateResult<WriteResponse> result = new StateResult<>(null);
        // TODO response
        result.putEventData("type", String.valueOf(request.getType()));
        result.putEventData("key", new String(request.getKey(), StandardCharsets.UTF_8));
        if (request.getValue() != null) {
            result.putEventData("value", new String(request.getValue(), StandardCharsets.UTF_8));
        }
        return result;
    }

    @Override
    public ReadResponse query(ReadRequest request, RaftJournal raftJournal) {
        return handler.handle(request);
    }
}
