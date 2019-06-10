package com.jd.journalkeeper.coordinating.state.domain;

/**
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public enum StateTypes {

    SET(0),

    GET(1),

    REMOVE(2),

    EXIST(3),

    COMPARE_AND_SET(4),

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
                return SET;
            case 1:
                return GET;
            case 2:
                return REMOVE;
            case 3:
                return EXIST;
            case 4:
                return COMPARE_AND_SET;
            default:
                throw new UnsupportedOperationException(String.valueOf(type));
        }
    }
}