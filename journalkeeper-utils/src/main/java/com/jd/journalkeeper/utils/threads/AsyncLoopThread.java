package com.jd.journalkeeper.utils.threads;

import com.jd.journalkeeper.utils.state.StateServer;

/**
 * @author liyue25
 * Date: 2019-06-21
 */
public interface AsyncLoopThread extends Runnable, StateServer {
    String getName();
    boolean isDaemon();
    void wakeup();
}
