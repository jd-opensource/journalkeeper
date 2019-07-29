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
package com.jd.journalkeeper.rpc.client;

import com.jd.journalkeeper.rpc.BaseResponse;
import com.jd.journalkeeper.rpc.StatusCode;
import com.jd.journalkeeper.utils.event.PullEvent;

import java.util.Collections;
import java.util.List;

/**
 * RPC 方法
 * {@link ClientServerRpc#pullEvents(PullEventsRequest) pullEvents{}}
 * 返回响应。
 *
 * 返回从上次ack 的序号至今的所有事件，保证事件有序。
 * 如果没有事件返回长度为0的List。
 *
 * StatusCode:
 * StatusCode.PULL_WATCH_ID_NOT_EXISTS: 监听ID不存在。
 * @author liyue25
 * Date: 2019-04-22
 */
public class PullEventsResponse extends BaseResponse {
    private final List<PullEvent> pullEvents;

    public PullEventsResponse(List<PullEvent> pullEvents) {
        if(null != pullEvents) {
            setStatusCode(StatusCode.SUCCESS);
        } else {
            setStatusCode(StatusCode.PULL_WATCH_ID_NOT_EXISTS);
        }
        this.pullEvents = pullEvents;
    }

    /**
     * 返回的事件列表
     * @return 返回的事件列表
     */
    public List<PullEvent> getPullEvents() {
        return pullEvents;
    }

    public PullEventsResponse(Throwable throwable) {
        super(throwable);
        this.pullEvents = Collections.emptyList();
    }
}
