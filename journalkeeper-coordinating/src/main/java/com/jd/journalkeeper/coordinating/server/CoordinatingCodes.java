package com.jd.journalkeeper.coordinating.server;

/**
 * CoordinatingCodes
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/5
 */
public enum CoordinatingCodes {

    // 公共类
    SUCCESS(100),
    ERROR(101),

    // 错误
    KEY_NOT_EXIST(200),
    EXPECT_VALUE_ERROR(200),

    ;

    private int type;

    CoordinatingCodes(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}