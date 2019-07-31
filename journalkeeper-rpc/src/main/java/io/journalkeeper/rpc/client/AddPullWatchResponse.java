/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.rpc.client;

import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.StatusCode;

/**
 * RPC 方法
 * {@link ClientServerRpc#addPullWatch() addPullWatch()}
 * 返回响应。
 *
 * @author LiYue
 * Date: 2019-04-19
 */
public class AddPullWatchResponse extends BaseResponse {
    private final long pullWatchId;
    private final long pullIntervalMs;

    public AddPullWatchResponse(long pullWatchId, long pullIntervalMs) {
        super(StatusCode.SUCCESS);
        this.pullWatchId = pullWatchId;
        this.pullIntervalMs = pullIntervalMs;
    }

    public AddPullWatchResponse(Throwable throwable) {
        super(throwable);
        this.pullIntervalMs = -1L;
        this.pullWatchId = -1L;
    }

    /**
     * 监听ID
     * @return 监听ID
     */
    public long getPullWatchId() {
        return pullWatchId;
    }

    /**
     * 获取拉取监听事件的时间间隔。
     * @return 监听时间间隔，单位毫秒。
     */
    public long getPullIntervalMs() {
        return pullIntervalMs;
    }
}
