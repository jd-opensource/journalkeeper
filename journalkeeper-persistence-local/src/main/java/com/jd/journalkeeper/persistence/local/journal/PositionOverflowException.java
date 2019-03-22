package com.jd.journalkeeper.persistence.local.journal;

/**
 * @author liyue25
 * Date: 2018-12-12
 */
class PositionOverflowException extends RuntimeException {

    PositionOverflowException(long position, long right) {
        super(String.format("Read position %d should be less than store right position %d.", position, right));
    }
}
