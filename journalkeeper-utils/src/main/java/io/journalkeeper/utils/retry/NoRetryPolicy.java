package io.journalkeeper.utils.retry;

/**
 * @author LiYue
 * Date: 2019/10/16
 */
public class NoRetryPolicy implements RetryPolicy {
    @Override
    public long getRetryDelayMs(int retries) {
        return -1L;
    }
}
