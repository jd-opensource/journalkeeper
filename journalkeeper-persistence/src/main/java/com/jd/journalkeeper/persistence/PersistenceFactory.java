package com.jd.journalkeeper.persistence;


/**
 * @author liyue25
 * Date: 2019-03-20
 */
public interface PersistenceFactory {
    MetadataPersistence createMetadataPersistenceInstance();
    JournalPersistence createJournalPersistenceInstance();

}
