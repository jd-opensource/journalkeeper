package com.jd.journalkeeper.coordinating.state;

import com.jd.journalkeeper.coordinating.state.config.CoordinatingConfigs;
import com.jd.journalkeeper.coordinating.state.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.state.domain.StateResponse;
import com.jd.journalkeeper.coordinating.state.domain.StateWriteRequest;
import com.jd.journalkeeper.coordinating.state.store.KVStore;
import com.jd.journalkeeper.coordinating.state.store.KVStoreManager;
import com.jd.journalkeeper.core.api.RaftJournal;
import com.jd.journalkeeper.core.api.StateFactory;
import com.jd.journalkeeper.core.state.LocalState;

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
 * email: gaohaoxiang@jd.com
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
