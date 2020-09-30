/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.utils.retry;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Refer to https://aws.amazon.com/cn/blogs/architecture/exponential-backoff-and-jitter/
 */
public class ExponentialRetryPolicy implements RetryPolicy{
    private final long baseMs;
    private final long capMs;
    private final int maxRetries;

    /**
     * 指数退避加上抖动的重试策略。
     * @param baseMs 基础重试时延
     * @param capMs 重试时延上限
     * @param maxRetries 最多重试次数
     */
    public ExponentialRetryPolicy(long baseMs, long capMs, int maxRetries) {
        this.baseMs = baseMs;
        this.capMs = capMs;
        this.maxRetries = maxRetries;
    }

    @Override
    public long getRetryDelayMs(int retries) {
        // wait = random_between(0, min(cap, base * 2 ** attempt))
        if(retries <= 0) {
            return 0L;
        } else if(retries < maxRetries ) {
            return ThreadLocalRandom.current().nextLong(0, Math.min(capMs, baseMs * (int) Math.pow(2, retries)));
        } else {
            return STOP_RETRY;
        }
    }

}
