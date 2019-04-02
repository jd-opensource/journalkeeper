package com.jd.journalkeeper.rpc;

import java.util.HashMap;
import java.util.Map;

/**
 * Response 状态码
 * 范围：0 ~ 254
 * @author liyue25
 * Date: 2019-03-29
 */
public enum StatusCode {


    SUCCESS(0, "SUCCESS"),

    //001 ~ 100 Common
    UNKNOWN_ERROR(1, "UNKNOWN_ERROR"),
    EXCEPTION(2, "EXCEPTION"),
    NOT_LEADER(100, "NOT_LEADER"),

    // 101 ~ 200 Client Server Rpc


    // 201 ~ 254 Server Rpc
    INDEX_UNDERFLOW(201, "INDEX_UNDERFLOW"),
    INDEX_OVERFLOW(202, "INDEX_OVERFLOW");

    private static Map<Integer, StatusCode> codes = new HashMap<>();
    private int code;
    private String message;

    static {
        for (StatusCode jmqCode : StatusCode.values()) {
            codes.put(jmqCode.code, jmqCode);
        }
    }

    StatusCode(int code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * 获取错误代码
     *
     * @param code
     * @return
     */
    public static StatusCode valueOf(int code) {
        return codes.get(code);
    }

    public int getCode() {
        return code;
    }

    public String getMessage(Object... args) {
        if (args.length < 1) {
            return message;
        }
        return String.format(message, args);
    }
}
