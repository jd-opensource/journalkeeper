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
package io.journalkeeper.rpc;

import io.journalkeeper.exceptions.NotLeaderException;

import java.net.URI;

/**
 * 请求LEADER方法的通用Response。
 * 当请求的节点不是LEADER时：
 * statusCode == StatusCode.NOT_LEADER
 * getLeader() 返回当前LEADER的URI。
 *
 * @author LiYue
 * Date: 2019-03-29
 */
public abstract class LeaderResponse extends BaseResponse {
    protected URI leader;

    public LeaderResponse () {}
    public LeaderResponse(Throwable throwable) {
        super(throwable);
    }

    public LeaderResponse(StatusCode statusCode) {
        super(statusCode);
    }

    @Override
    protected void onSetException(Throwable throwable) {
        try {
            throw throwable;
        } catch (NotLeaderException e) {
            setStatusCode(StatusCode.NOT_LEADER);
            setLeader(e.getLeader());
        } catch (Throwable t) {
            super.onSetException(throwable);
        }
    }

    /**
     * 当前LEADER的URI
     * @return 当前LEADER的URI
     */
    public URI getLeader() {
        return leader;
    }

    public void setLeader(URI leader) {
        this.leader = leader;
    }
}
