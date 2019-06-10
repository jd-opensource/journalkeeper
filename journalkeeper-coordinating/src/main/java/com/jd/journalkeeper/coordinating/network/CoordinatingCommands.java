package com.jd.journalkeeper.coordinating.network;

/**
 * CoordinatingCommands
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public enum CoordinatingCommands {

    BOOLEAN_RESPONSE(0),

    // 公共类
    GET_CLUSTER_REQUEST(100),
    GET_CLUSTER_RESPONSE(-100),
    HEARTBEAT_REQUEST(101),

    // 操作类
    PUT_REQUEST(200),
    PUT_RESPONSE(-200),
    GET_REQUEST(201),
    GET_RESPONSE(-201),
    REMOVE_REQUEST(202),
    EXIST_REQUEST(203),
    EXIST_RESPONSE(-203),
    COMPARE_AND_SET_REQUEST(204),
    COMPARE_AND_SET_RESPONSE(-204),

    // 监听类
    WATCH_REQUEST(300),
    UN_WATCH_REQUEST(301),
    PUBLISH_WATCH_REQUEST(302),

    ;

    private int type;

    CoordinatingCommands(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}