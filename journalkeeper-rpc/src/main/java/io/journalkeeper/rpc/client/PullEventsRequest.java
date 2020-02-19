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
package io.journalkeeper.rpc.client;

/**
 * RPC 方法
 * {@link ClientServerRpc#pullEvents(PullEventsRequest) pullEvents()}
 * 请求参数
 * @author LiYue
 * Date: 2019-04-22
 */
public class PullEventsRequest {
    private final long pullWatchId;
    private final long ackSequence;

    public PullEventsRequest(long pullWatchId, long ackSequence) {
        this.pullWatchId = pullWatchId;
        this.ackSequence = ackSequence;
    }

    /**
     * 获取监听ID
     * @return 监听ID
     */
    public long getPullWatchId() {
        return pullWatchId;
    }

    /**
     * 获取确认位置，用于确认已收到的事件。
     * @return 确认位置。如果确认位置小于0，则本次请求不进行确认事件操作。
     */
    public long getAckSequence() {
        return ackSequence;
    }
}
