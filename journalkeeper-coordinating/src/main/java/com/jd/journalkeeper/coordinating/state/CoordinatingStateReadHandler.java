package com.jd.journalkeeper.coordinating.state;

import com.jd.journalkeeper.coordinating.state.domain.StateCodes;
import com.jd.journalkeeper.coordinating.state.domain.StateReadRequest;
import com.jd.journalkeeper.coordinating.state.domain.StateResponse;
import com.jd.journalkeeper.coordinating.state.domain.StateTypes;
import com.jd.journalkeeper.coordinating.state.store.KVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * CoordinatingStateReadHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/11
 */
public class CoordinatingStateReadHandler {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingStateReadHandler.class);

    private Properties properties;
    private KVStore kvStore;

    public CoordinatingStateReadHandler(Properties properties, KVStore kvStore) {
        this.properties = properties;
        this.kvStore = kvStore;
    }

    public StateResponse handle(StateReadRequest request) {
        try {
            StateTypes type = StateTypes.valueOf(request.getType());
            switch (type) {
                case GET: {
                    return doGet(request.getKey());
                }
                case EXIST: {
                    return doExist(request.getKey());
                }
                case LIST: {
                    return doList(request.getKeys());
                }
                default: {
                    logger.warn("unsupported type, type: {}, request: {}", type, request);
                    return null;
                }
            }
        } catch (Exception e) {
            logger.error("handle read request exception, request: {}", request, e);
            return new StateResponse(StateCodes.ERROR.getCode(), e.toString());
        }
    }

    protected StateResponse doGet(byte[] key) {
        byte[] value = kvStore.get(key);
        return new StateResponse(StateCodes.SUCCESS.getCode(), value);
    }

    protected StateResponse doExist(byte[] key) {
        boolean isExist = kvStore.exist(key);
        return new StateResponse(StateCodes.SUCCESS.getCode(), (isExist ? new byte[] {1} : new byte[] {0}));
    }

    protected StateResponse doList(List<byte[]> keys) {
        List<byte[]> values = kvStore.multiGet(keys);
        return new StateResponse(StateCodes.SUCCESS.getCode(), new ArrayList<>(values));
    }
}