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
package io.journalkeeper.core.state;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.journalkeeper.core.serialize.WrappedState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 基于HashMap，使用JSON序列化存储的简易KV存储。
 * @author LiYue
 * Date: 2019-04-03
 */
public class KvState implements WrappedState<String, String, String, String>, Flushable {
    private static final Logger logger = LoggerFactory.getLogger(KvState.class);
    private final static String FILENAME = "map";
    private static final String CMD_GET = "GET";
    private static final String CMD_SET = "SET";
    private static final String CMD_DEL = "DEL";
    private static final String CMD_LIST = "KEYS";
    private final Gson gson = new Gson();
    private Map<String, String> stateMap = new HashMap<>();
    private Path statePath;
    private final AtomicBoolean isDirty = new AtomicBoolean(false);

    @Override
    public void recover(Path statePath, Properties properties) {
        this.statePath = statePath;
        try {
            stateMap = gson.fromJson(new String(Files.readAllBytes(statePath.resolve(FILENAME)), StandardCharsets.UTF_8),
                    new TypeToken<HashMap<String, String>>() {
                    }.getType());
            int keys = stateMap == null ? -1 : stateMap.size();
            isDirty.set(false);
            logger.info("State map recovered from {}, keys {} ", statePath.toString(), keys);
        } catch (NoSuchFileException e) {
            stateMap = new HashMap<>();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkInput(String[] input, int count) {
        if (input.length < count) {
            throw new IllegalArgumentException("Bad request: " + String.join(" ", input) + "!");
        }
    }

    @Override
    public String execute(String cmd) {
        String[] splt = cmd.split("\\s");
        try {
            checkInput(splt, 1);

            if (CMD_SET.equals(splt[0])) {
                checkInput(splt, 3);
                stateMap.put(splt[1], splt[2]);
                isDirty.compareAndSet(false, true);
                return null;
            }

            if (CMD_DEL.equals(splt[0])) {
                checkInput(splt, 2);
                stateMap.remove(splt[1]);
                isDirty.compareAndSet(false, true);
                return null;
            }
            throw new IllegalArgumentException("Unknown command: " + cmd + "!");
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @Override
    public String query(String query) {

        String[] splt = query.split("\\s");
        try {
            checkInput(splt, 1);

            if (CMD_GET.equals(splt[0])) {
                checkInput(splt, 2);
                return stateMap.get(splt[1]);
            }
            if (CMD_LIST.equals(splt[0])) {
                return String.join(", ", stateMap.keySet());
            }
            throw new IllegalArgumentException("Unknown command: " + query + "!");
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @Override
    public void flush() throws IOException {
        if(isDirty.compareAndSet(true, false)) {
            Files.write(statePath.resolve(FILENAME), gson.toJson(stateMap).getBytes(StandardCharsets.UTF_8));
        }
    }
}