package io.journalkeeper.utils.retry;

/**
 * @author LiYue
 * Date: 2019-09-17
 */
public interface CheckRetry<R> {
    boolean checkException(Throwable exception);
    boolean checkResult(R result);
}
