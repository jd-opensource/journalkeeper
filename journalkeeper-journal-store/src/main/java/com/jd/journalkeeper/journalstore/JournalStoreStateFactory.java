package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.api.State;
import com.jd.journalkeeper.core.api.StateFactory;

/**
 * @author liyue25
 * Date: 2019-05-09
 */
public class JournalStoreStateFactory implements StateFactory<byte [], JournalStoreQuery, JournalStoreQueryResult> {
    @Override
    public State<byte [], JournalStoreQuery, JournalStoreQueryResult> createState() {
        return new JournalStoreState(this);
    }
}
