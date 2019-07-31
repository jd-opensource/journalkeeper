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
import io.journalkeeper.coordinating.state.domain.StateReadRequest;
import io.journalkeeper.coordinating.state.domain.StateResponse;
import io.journalkeeper.coordinating.state.domain.StateWriteRequest;
import io.journalkeeper.coordinating.state.store.KVStore;
import io.journalkeeper.coordinating.state.store.KVStoreManager;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.api.StateFactory;
import io.journalkeeper.core.state.LocalState;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * CoordinatingState
 * author: gaohaoxiang
 *
 * date: 2019/5/30
 */
public class CoordinatingState extends LocalState<StateWriteRequest, StateReadRequest, StateResponse> {

    private Properties properties;
    private KVStore kvStore;
    private CoordinatingStateHandler handler;

    protected CoordinatingState(StateFactory<StateWriteRequest, StateReadRequest, StateResponse> stateFactory) {
        super(stateFactory);
    }

    @Override
    protected void recoverLocalState(Path path, RaftJournal raftJournal, Properties properties) throws IOException {
        this.properties = properties;
        this.kvStore = KVStoreManager.getFactory(properties.getProperty(CoordinatingConfigs.STATE_STORE)).create(path, properties);
        this.handler = new CoordinatingStateHandler(properties, kvStore);
    }

    @Override
    public Map<String, String> execute(StateWriteRequest entry, int partition, long index, int batchSize) {
        boolean isSuccess = handler.handle(entry);
        if (!isSuccess) {
            return null;
        }

        Map<String, String> parameters = new HashMap<>();
        parameters.put("type", String.valueOf(entry.getType()));
        parameters.put("key", new String(entry.getKey(), Charset.forName("UTF-8")));
        if (entry.getValue() != null) {
            parameters.put("value", new String(entry.getValue(), Charset.forName("UTF-8")));
        }
        return parameters;
    }

    @Override
    public CompletableFuture<StateResponse> query(StateReadRequest query) {
        return CompletableFuture.supplyAsync(() -> {
            return handler.handle(query);
        });
    }
}
