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

/**
 * @author LiYue
 * Date: 2019/10/9
 */
public interface RetryPolicy {
    /**
     * 计算下次重试之前等待的时间（毫秒）
     * @param retries 已重试的次数（不含即将执行的下次重试）
     * @return 如果返回值小于0：不再重试，返回上一次的执行结果。如果返回值大于等于0，等待后继续重试。
     */
    long getRetryDelayMs(int retries);
}
