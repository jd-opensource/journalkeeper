package com.jd.journalkeeper.core.server;

import com.jd.journalkeeper.base.Replicable;
import com.jd.journalkeeper.core.api.StateMachine;

import java.util.concurrent.locks.StampedLock;

/**
 * 状态和状态对应的位置，保证同步更新
 * @author liyue25
 * Date: 2019-03-15
 */
public class StateWithIndex<E, S extends Replicable<S>> {
    private final StampedLock sl = new StampedLock();
    private S state;
    private long index;

    public S getState() {
        return state;
    }

    public long getIndex() {
        return index;
    }

    public StateWithIndex<E, S> takeSnapshot() {
        StateWithIndex<E, S> snapshot = new StateWithIndex<>();
        long stamp = sl.tryOptimisticRead();
        snapshot.setUnsafe(this.state, this.index);
        if(!sl.validate(stamp)) {
            stamp = sl.readLock();
            try {
              snapshot.setUnsafe(this.state, this.index);
            } finally {
                sl.unlockRead(stamp);
            }
        }
        return snapshot;
    }

    public void set(S state, long index) {
        long stamp = sl.writeLock();
        try {
            setUnsafe(state, index);
        } finally {
            sl.unlockWrite(stamp);
        }
    }
    private void setUnsafe(S state, long index) {
        this.state = state;
        this.index = index;
    }
    public void execute(E entry, StateMachine<E, S> stateMachine) {
        long stamp = sl.writeLock();
        try {
            S newState = stateMachine.execute(state, entry);
            if(newState != state) {
                state = newState;
            }
            index++;
        } finally {
            sl.unlockWrite(stamp);
        }
    }
}
