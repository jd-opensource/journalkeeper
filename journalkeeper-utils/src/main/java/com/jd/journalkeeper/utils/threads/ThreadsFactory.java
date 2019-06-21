package com.jd.journalkeeper.utils.threads;

/**
 * @author liyue25
 * Date: 2019-06-21
 */
public class ThreadsFactory {
    public static Threads create() {
        return new ThreadsManager();
    }
}
