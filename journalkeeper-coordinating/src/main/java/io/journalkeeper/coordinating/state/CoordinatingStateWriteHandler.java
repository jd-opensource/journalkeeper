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

import io.journalkeeper.coordinating.state.domain.StateCodes;
import io.journalkeeper.coordinating.state.domain.StateTypes;
import io.journalkeeper.coordinating.state.domain.WriteRequest;
import io.journalkeeper.coordinating.state.domain.WriteResponse;
import io.journalkeeper.coordinating.state.store.KVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * CoordinatingStateWriteHandler
 * author: gaohaoxiang
 *
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

    public WriteResponse handle(WriteRequest request) {
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
                    return new WriteResponse(StateCodes.ERROR.getCode(), "unsupported type");
                }
            }
        } catch (Exception e) {
            logger.error("handle write request exception, request: {}", request, e);
            return new WriteResponse(StateCodes.ERROR.getCode(), e.getMessage());
        }
    }

    protected WriteResponse doSet(byte[] key, byte[] value) {
        boolean result = kvStore.set(key, value);
        return new WriteResponse(StateCodes.SUCCESS.getCode(), String.valueOf(result));
    }

    protected WriteResponse doRemove(byte[] key) {
        boolean result = kvStore.remove(key);
        return new WriteResponse(StateCodes.SUCCESS.getCode(), String.valueOf(result));
    }

    protected WriteResponse doCompareAndSet(byte[] key, byte[] expect, byte[] update) {
        boolean result = kvStore.compareAndSet(key, expect, update);
        return new WriteResponse(StateCodes.SUCCESS.getCode(), String.valueOf(result));
    }
}