package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.base.Serializer;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class JournalStoreQueryResultSerializer implements Serializer<JournalStoreQueryResult> {
    @Override
    public int sizeOf(JournalStoreQueryResult journalStoreQueryResult) {
        return 0;
    }

    @Override
    public byte[] serialize(JournalStoreQueryResult entry) {
        return new byte[0];
    }

    @Override
    public JournalStoreQueryResult parse(byte[] bytes) {
        return null;
    }
}
