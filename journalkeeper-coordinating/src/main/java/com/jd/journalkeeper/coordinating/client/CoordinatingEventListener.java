package com.jd.journalkeeper.coordinating.client;

/**
 * CoordinatingEventListener
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/11
 */
public interface CoordinatingEventListener {

    void onEvent(CoordinatingEvent event);
}