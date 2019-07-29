package com.jd.journalkeeper.coordinating.state.domain;

/**
 * StateCodes
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/5/30
 */
public enum StateCodes {

    SUCCESS(0),

    ERROR(1),

    ;

    private int code;

    StateCodes(int type) {
        this.code = type;
    }

    public int getCode() {
        return code;
    }

    public static StateCodes valueOf(int type) {
        switch (type) {
            case 0:
                return SUCCESS;
            case 1:
                return ERROR;
            default:
                throw new UnsupportedOperationException(String.valueOf(type));
        }
    }
}