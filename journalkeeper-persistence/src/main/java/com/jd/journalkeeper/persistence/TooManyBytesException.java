package com.jd.journalkeeper.persistence;

import java.nio.file.Path;

/**
 * append的数据太大时抛出
 * @author liyue25
 * Date: 2019-04-04
 */
public class TooManyBytesException extends RuntimeException {
    public TooManyBytesException() {
        super();
    }

    public TooManyBytesException(int length, int limit, Path path) {
        super(String.format("Append bytes length %d exceed limit %d, path: %s.", length, limit, path.toString()));
    }
}
