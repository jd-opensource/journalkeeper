package com.jd.journalkeeper.persistence.local.journal;

/**
 * @author liyue25
 * Date: 2018-12-12
 */
class PositionUnderflowException extends RuntimeException {

    PositionUnderflowException(long position, long left) {
        super(String.format("Read position %d should be greater than or equal to store left position %d.", position, left));
    }

}
