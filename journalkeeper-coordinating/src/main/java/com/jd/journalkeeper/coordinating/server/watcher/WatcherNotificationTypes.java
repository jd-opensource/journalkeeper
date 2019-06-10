package com.jd.journalkeeper.coordinating.server.watcher;

/**
 * WatcherNotificationTypes
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/6
 */
public enum WatcherNotificationTypes {

    KEY_CHANGED(0),

    KEY_REMOVED(1)

    ;

    private int type;

    WatcherNotificationTypes(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}