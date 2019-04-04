package com.jd.journalkeeper.persistence.local;

import com.jd.journalkeeper.persistence.JournalPersistence;
import com.jd.journalkeeper.persistence.MetadataPersistence;
import com.jd.journalkeeper.persistence.PersistenceFactory;
import com.jd.journalkeeper.persistence.local.journal.PositioningStore;
import com.jd.journalkeeper.persistence.local.metadata.MetadataStore;
import com.jd.journalkeeper.utils.buffer.PreloadBufferPool;

import java.io.Closeable;
import java.io.IOException;

public class StoreFactory implements PersistenceFactory, Closeable {

    private final PreloadBufferPool preloadBufferPool = new PreloadBufferPool();

    @Override
    public MetadataPersistence createMetadataPersistenceInstance() {
        return new MetadataStore();
    }

    @Override
    public JournalPersistence createJournalPersistenceInstance() {
        return new PositioningStore(preloadBufferPool);
    }

    @Override
    public void close() {
        preloadBufferPool.close();
    }
}
