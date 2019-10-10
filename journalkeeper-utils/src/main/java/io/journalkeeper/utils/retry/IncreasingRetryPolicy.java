package io.journalkeeper.utils.retry;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author LiYue
 * Date: 2019/10/9
 */
public class IncreasingRetryPolicy implements RetryPolicy {
    private final long [] retryDelayArray;
    private final long randomFactorMs;

    public IncreasingRetryPolicy(long[] retryDelayArray, long randomFactorMs) {
        this.retryDelayArray = retryDelayArray;
        this.randomFactorMs = randomFactorMs;
    }

    @Override
    public long getRetryDelayMs(int retries) {
        if(retries == 0) return 0;
        int index = retries - 1;
        if(index < retryDelayArray.length) {
            return retryDelayArray[index] + ThreadLocalRandom.current().nextLong(randomFactorMs);
        } else {
            return -1L;
        }
    }
}
