package com.jd.journalkeeper.examples.kv;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.jd.journalkeeper.core.api.RaftJournal;
import com.jd.journalkeeper.core.state.LocalState;
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
 * @author liyue25
 * Date: 2019-04-03
 */
public class KvState extends LocalState<KvEntry, KvQuery, KvResult> {
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
    public Map<String, String> execute(KvEntry entry, int partition, long index, int batchSize) {
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
                        return new KvResult(stateMap.get(query.getKey()), null);
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
