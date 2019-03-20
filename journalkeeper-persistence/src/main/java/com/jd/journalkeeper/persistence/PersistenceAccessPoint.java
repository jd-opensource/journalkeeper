package com.jd.journalkeeper.persistence;

import java.util.Properties;

/**
 * @author liyue25
 * Date: 2019-03-20
 */
public interface PersistenceAccessPoint {
    MetadataPersistence getMetadataPersistence(Properties properties);
    JournalPersistence getJournalPersistence(Properties properties);

}
