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
 * @author LiYue
 * Date: 2019/10/9
 */
public class IncreasingRetryPolicy implements RetryPolicy {
    private final long[] retryDelayArray;
    private final long randomFactorMs;

    public IncreasingRetryPolicy(long[] retryDelayArray, long randomFactorMs) {
        this.retryDelayArray = retryDelayArray;
        this.randomFactorMs = randomFactorMs;
    }

    @Override
    public long getRetryDelayMs(int retries) {
        if (retries == 0) return 0;
        int index = retries - 1;
        if (index < retryDelayArray.length) {
            return retryDelayArray[index] + ThreadLocalRandom.current().nextLong(randomFactorMs);
        } else {
            return -1L;
        }
    }
}
