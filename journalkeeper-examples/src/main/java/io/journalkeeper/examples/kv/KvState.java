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
package io.journalkeeper.examples.kv;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.journalkeeper.core.api.RaftJournal;
import io.journalkeeper.core.state.LocalState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;


/**
 * 基于HashMap，使用JSON序列化存储的简易KV存储。
 * @author LiYue
 * Date: 2019-04-03
 */
public class KvState extends LocalState<KvEntry, Void, KvQuery, KvResult> {
    private static final Logger logger = LoggerFactory.getLogger(KvState.class);
    private Map<String, String> stateMap = new HashMap<>();
    private final static String FILENAME = "map";
    private final Gson gson = new Gson();
    private Path statePath;
    public KvState(KvStateFactory stateFactory) {
        super(stateFactory);
    }

    @Override
    protected void recoverLocalState(Path statePath, RaftJournal raftJournal, Properties properties) {
        this.statePath = statePath;
        try {
            stateMap =  gson.fromJson(new String(Files.readAllBytes(statePath.resolve(FILENAME)), StandardCharsets.UTF_8),
                    new TypeToken<HashMap<String, String>>(){}.getType());
            int keys  = stateMap == null ? -1 : stateMap.size();
            logger.info("State map recovered from {}, keys {} ", statePath.toString(), keys);
        } catch (NoSuchFileException e) {
            stateMap = new HashMap<>();
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Void execute(KvEntry entry, int partition, long index, int batchSize, Map<String, String> eventParams) {
        switch (entry.getCmd()) {
            case KvEntry.CMD_SET:
                stateMap.put(entry.getKey(), entry.getValue());
                break;
            case KvEntry.CMD_DEL:
                stateMap.remove(entry.getKey());
                break;
        }
        try {
            Files.write(statePath.resolve(FILENAME), gson.toJson(stateMap).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            logger.warn("Exception:", e);
        }
        return null;
    }

    @Override
    public CompletableFuture<KvResult> query(KvQuery query) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                switch (query.getCmd()) {
                    case KvQuery.CMD_GET:
                        logger.info("Query: {}", query.getKey());
                        KvResult result = new KvResult(stateMap.get(query.getKey()), null);
                        logger.info("Result: {}", result.getValue());
                        return result;
                    case KvQuery.CMD_LIST_KEYS:
                        return new KvResult(null, new ArrayList<>(stateMap.keySet()));
                    default:
                        return new KvResult();
                }
            } catch (NullPointerException e) {
                logger.warn("NullPointer Exception: statMap is null: {}, query.getKey() is null: {}", stateMap == null, query.getKey() == null);
                throw e;
            }
        });
    }
}
