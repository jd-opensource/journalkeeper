package com.jd.journalkeeper.coordinating.state;

import com.jd.journalkeeper.coordinating.state.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.state.domain.StateResponse;
import com.jd.journalkeeper.coordinating.state.domain.StateWriteRequest;
import com.jd.journalkeeper.coordinating.state.store.KVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * CoordinatingStateHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/11
 */
public class CoordinatingStateHandler {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingStateHandler.class);

    private Properties properties;
    private KVStore kvStore;
    private CoordinatingStateWriteHandler writeHandler;
    private CoordinatingStateReadHandler readHandler;

    public CoordinatingStateHandler(Properties properties, KVStore kvStore) {
        this.properties = properties;
        this.kvStore = kvStore;
        this.writeHandler = new CoordinatingStateWriteHandler(properties, kvStore);
        this.readHandler = new CoordinatingStateReadHandler(properties, kvStore);
    }

    public boolean handle(StateWriteRequest request) {
        return writeHandler.handle(request);
    }

    public StateResponse handle(StateReadRequest request) {
        return readHandler.handle(request);
    }
}