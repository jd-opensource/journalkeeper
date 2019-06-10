package com.jd.journalkeeper.coordinating.server.config;

/**
 * CoordinatingWatcherConfig
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/10
 */
public class CoordinatingWatcherConfig {

    private int publishTimeout = 1000 * 3;

    public void setPublishTimeout(int publishTimeout) {
        this.publishTimeout = publishTimeout;
    }

    public int getPublishTimeout() {
        return publishTimeout;
    }
}