package com.jd.journalkeeper.coordinating.state;

import com.jd.journalkeeper.coordinating.state.domain.StateTypes;
import com.jd.journalkeeper.coordinating.state.domain.StateWriteRequest;
import com.jd.journalkeeper.coordinating.state.store.KVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * CoordinatingStateWriteHandler
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/11
 */
public class CoordinatingStateWriteHandler {

    protected static final Logger logger = LoggerFactory.getLogger(CoordinatingStateWriteHandler.class);

    private Properties properties;
    private KVStore kvStore;

    public CoordinatingStateWriteHandler(Properties properties, KVStore kvStore) {
        this.properties = properties;
        this.kvStore = kvStore;
    }

    // TODO 临时测试
    public boolean handle(StateWriteRequest request) {
        try {
            StateTypes type = StateTypes.valueOf(request.getType());
            switch (type) {
                case SET: {
                    return doSet(request.getKey(), request.getValue());
                }
                case REMOVE: {
                    return doRemove(request.getKey());
                }
                case COMPARE_AND_SET: {
                    return doCompareAndSet(request.getKey(), request.getExpect(), request.getValue());
                }
                default: {
                    logger.warn("unsupported type, type: {}, request: {}", type, request);
                    return false;
                }
            }
        } catch (Exception e) {
            logger.error("handle write request exception, request: {}", request, e);
            return false;
        }
    }

    protected boolean doSet(byte[] key, byte[] value) {
        return kvStore.set(key, value);
    }

    protected boolean doRemove(byte[] key) {
        return kvStore.remove(key);
    }

    protected boolean doCompareAndSet(byte[] key, byte[] expect, byte[] update) {
        return kvStore.compareAndSet(key, expect, update);
    }
}