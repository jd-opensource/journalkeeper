package com.jd.journalkeeper.examples.kv;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.jd.journalkeeper.core.state.LocalState;

import java.io.Flushable;
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
    private Map<String, String> stateMap = new HashMap<>();
    private final static String FILENAME = "map";
    private final Gson gson = new Gson();

    public KvState(KvStateFactory stateFactory) {
        super(stateFactory);
    }

    @Override
    protected void recoverLocalState(Path statePath, Properties properties) {
        try {
            stateMap =  gson.fromJson(new String(Files.readAllBytes(statePath.resolve(FILENAME)), StandardCharsets.UTF_8),
                    new TypeToken<HashMap<String, String>>(){}.getType());
        } catch (NoSuchFileException e) {
            stateMap = new HashMap<>();
        }catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void flushState(Path statePath) throws IOException {
        Files.write(statePath.resolve(FILENAME),gson.toJson(stateMap).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void execute(KvEntry entry) {
        switch (entry.getCmd()) {
            case KvEntry.CMD_SET:
                stateMap.put(entry.getKey(), entry.getValue());
                break;
            case KvEntry.CMD_DEL:
                stateMap.remove(entry.getKey());
                break;
        }
        lastApplied ++;
    }

    @Override
    public CompletableFuture<KvResult> query(KvQuery query) {
        return CompletableFuture.supplyAsync(() -> {
            switch (query.getCmd()) {
                case KvQuery.CMD_GET:
                    return new KvResult(stateMap.get(query.getKey()), null);
                case KvQuery.CMD_LIST_KEYS:
                    return new KvResult(null, new ArrayList<>(stateMap.keySet()));
                default:
                    return new KvResult();
            }
        });
    }
}
