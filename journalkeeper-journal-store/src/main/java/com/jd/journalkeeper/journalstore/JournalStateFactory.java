package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.api.State;
import com.jd.journalkeeper.core.api.StateFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-04-23
 */
public class JournalStateFactory implements StateFactory<ByteBuffer, JournalStoreQuery, List<ByteBuffer>> {
    @Override
    public State<ByteBuffer, JournalStoreQuery, List<ByteBuffer>> createState() {
        return new JournalState(this);
    }
}
