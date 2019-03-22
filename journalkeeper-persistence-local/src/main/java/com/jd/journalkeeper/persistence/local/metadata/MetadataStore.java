package com.jd.journalkeeper.persistence.local.metadata;

import com.google.gson.Gson;
import com.jd.journalkeeper.persistence.MetadataPersistence;
import com.jd.journalkeeper.persistence.ServerMetadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * JSON 双写元数据，避免写入过程中损坏。
 */
public class MetadataStore implements MetadataPersistence {
    private final Gson gson = new Gson();
    private Path path = null;
    @Override
    public void save(ServerMetadata serverMetadata) throws IOException {
        byte [] serialized = gson.toJson(serverMetadata).getBytes(StandardCharsets.UTF_8);
        Files.write(path.resolve("0"), serialized);
        Files.write(path.resolve("1"), serialized);
    }

    @Override
    public ServerMetadata recover(Path path, Properties properties) throws IOException {
        this.path = path;
        try {
            return gson.fromJson(new String(Files.readAllBytes(path.resolve("0")), StandardCharsets.UTF_8), ServerMetadata.class);
        } catch (Throwable ignored) {
            return gson.fromJson(new String(Files.readAllBytes(path.resolve("1")), StandardCharsets.UTF_8), ServerMetadata.class);
        }
    }
}
