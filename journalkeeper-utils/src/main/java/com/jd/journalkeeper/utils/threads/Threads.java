package com.jd.journalkeeper.utils.threads;

import com.jd.journalkeeper.utils.state.StateServer;

/**
 * 一组线程的抽象
 * @author liyue25
 * Date: 2019-06-21
 */
public interface Threads extends StateServer {
    void createThread(AsyncLoopThread asyncThread);
    void wakeupThread(String name);
    void stopThread(String name);
    void startThread(String name);
    ServerState getTreadState(String name);
}
