package com.jd.journalkeeper.examples.kv;

import com.jd.journalkeeper.core.api.State;
import com.jd.journalkeeper.core.api.StateFactory;

/**
 * @author liyue25
 * Date: 2019-04-03
 */
public class KvStateFactory implements StateFactory<KvEntry, KvQuery, KvResult> {
    @Override
    public State<KvEntry, KvQuery, KvResult> createState() {
        return new KvState(this);
    }
}
