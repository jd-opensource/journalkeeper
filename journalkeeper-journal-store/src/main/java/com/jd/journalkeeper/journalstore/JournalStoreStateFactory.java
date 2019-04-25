package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.api.State;
import com.jd.journalkeeper.core.api.StateFactory;

import java.util.List;

/**
 * @author liyue25
 * Date: 2019-04-23
 */
public class JournalStoreStateFactory implements StateFactory<byte[], JournalStoreQuery, List<byte[]>> {
    @Override
    public State<byte[], JournalStoreQuery, List<byte[]>> createState() {
        return new JournalStoreState(this);
    }
}
