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
package io.journalkeeper.persistence.local.metadata;

import com.google.gson.Gson;
import io.journalkeeper.persistence.MetadataPersistence;
import io.journalkeeper.persistence.ServerMetadata;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * JSON 双写元数据，避免写入过程中损坏。
 */
public class MetadataStore implements MetadataPersistence {
    public static final String FIRST_COPY = "metadata.0";
    public static final String SECOND_COPY = "metadata.1";
    private final Gson gson = new Gson();
    private Path path = null;
    @Override
    public void save(ServerMetadata serverMetadata) throws IOException {
        byte [] serialized = gson.toJson(serverMetadata).getBytes(StandardCharsets.UTF_8);
        try {
            Files.write(path.resolve(FIRST_COPY), serialized);
            Files.write(path.resolve(SECOND_COPY), serialized);
        } catch (ClosedByInterruptException ignored) {}
    }

    @Override
    public ServerMetadata recover(Path path, Properties properties) throws IOException {

        this.path = path;
        if (!Files.isDirectory(path)) {
            Files.createDirectories(path);
        }
        if(Files.exists(path.resolve(FIRST_COPY)) || Files.exists(path.resolve(SECOND_COPY))) {
            try {
                return gson.fromJson(new String(Files.readAllBytes(path.resolve(FIRST_COPY)), StandardCharsets.UTF_8), ServerMetadata.class);
            } catch (Throwable ignored) {
                return gson.fromJson(new String(Files.readAllBytes(path.resolve(SECOND_COPY)), StandardCharsets.UTF_8), ServerMetadata.class);
            }
        } else {
            return new ServerMetadata();
        }
    }
}
