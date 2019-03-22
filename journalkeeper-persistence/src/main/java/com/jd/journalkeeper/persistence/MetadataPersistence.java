package com.jd.journalkeeper.persistence;

import java.nio.file.Path;
import java.util.Properties;

/**
 * 元数据存储
 * @author liyue25
 * Date: 2019-03-15
 */
public interface MetadataPersistence {
    void save(ServerMetadata serverMetadata);
    ServerMetadata recover(Path path, Properties properties);
}
