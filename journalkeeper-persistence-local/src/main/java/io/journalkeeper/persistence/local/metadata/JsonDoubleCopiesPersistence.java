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
package io.journalkeeper.persistence.local.metadata;

import com.google.gson.Gson;
import io.journalkeeper.persistence.MetadataPersistence;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * JSON 双写元数据，避免写入过程中损坏。
 */
public class JsonDoubleCopiesPersistence implements MetadataPersistence {
    static final String FIRST_COPY = ".0.json";
    static final String SECOND_COPY = ".1.json";
    private static final Gson gson = new Gson();

    @Override
    public <M> void save(Path path, M metadata) throws IOException {
        Files.createDirectories(path.getParent());
        byte[] serialized = gson.toJson(metadata).getBytes(StandardCharsets.UTF_8);
        try {
            Files.write(getCopy(path, FIRST_COPY), serialized);
            Files.write(getCopy(path, SECOND_COPY), serialized);
        } catch (ClosedByInterruptException ignored) {
        }
    }

    Path getCopy(Path path, String copy) {
        return path.getParent().resolve(path.getFileName() + copy);
    }

    @Override
    public <M> M load(Path path, Class<M> mClass) throws IOException {
        try {
            return gson.fromJson(new String(Files.readAllBytes(getCopy(path, FIRST_COPY)), StandardCharsets.UTF_8), mClass);
        } catch (Throwable ignored) {
            return gson.fromJson(new String(Files.readAllBytes(getCopy(path, SECOND_COPY)), StandardCharsets.UTF_8), mClass);
        }
    }
}
