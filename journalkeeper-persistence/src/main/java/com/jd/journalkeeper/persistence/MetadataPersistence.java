package com.jd.journalkeeper.persistence;

import java.util.concurrent.CompletableFuture;

/**
 * 元数据存储
 * @author liyue25
 * Date: 2019-03-15
 */
public interface MetadataPersistence {
    CompletableFuture<Void> save(byte [] metadata);
    CompletableFuture<byte []> load();
}
