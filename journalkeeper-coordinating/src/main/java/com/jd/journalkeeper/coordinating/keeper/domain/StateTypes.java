package com.jd.journalkeeper.coordinating.keeper.domain;

/**
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public enum StateTypes {

    PUT(0),

    GET(1),

    REMOVE(2),

    EXIST(3),

    ;

    private int type;

    StateTypes(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static StateTypes valueOf(int type) {
        switch (type) {
            case 0:
                return PUT;
            case 1:
                return GET;
            case 2:
                return REMOVE;
            case 3:
                return EXIST;
            default:
                throw new UnsupportedOperationException(String.valueOf(type));
        }
    }
}