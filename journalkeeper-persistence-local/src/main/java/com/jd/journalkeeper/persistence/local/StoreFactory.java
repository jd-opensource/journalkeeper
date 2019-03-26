package com.jd.journalkeeper.persistence.local;

import com.jd.journalkeeper.persistence.JournalPersistence;
import com.jd.journalkeeper.persistence.MetadataPersistence;
import com.jd.journalkeeper.persistence.PersistenceFactory;
import com.jd.journalkeeper.persistence.local.journal.PositioningStore;
import com.jd.journalkeeper.persistence.local.metadata.MetadataStore;
import com.jd.journalkeeper.utils.buffer.PreloadBufferPool;

public class StoreFactory implements PersistenceFactory {

    private final PreloadBufferPool preloadBufferPool = new PreloadBufferPool(1000L);

    @Override
    public MetadataPersistence createMetadataPersistenceInstance() {
        return new MetadataStore();
    }

    @Override
    public JournalPersistence createJournalPersistenceInstance() {
        return new PositioningStore(preloadBufferPool);
    }
}
