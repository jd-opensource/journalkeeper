package com.jd.journalkeeper.coordinating.state.state;

import com.jd.journalkeeper.core.api.State;
import com.jd.journalkeeper.core.api.StateFactory;

/**
 * CoordinatorStateFactory
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public class CoordinatorStateFactory implements StateFactory {

    @Override
    public State createState() {
        return new CoordinatingState(this);
    }
}