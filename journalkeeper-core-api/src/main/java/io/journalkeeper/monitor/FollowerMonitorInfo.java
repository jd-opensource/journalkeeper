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
package io.journalkeeper.monitor;

import io.journalkeeper.utils.state.StateServer;

/**
 * @author LiYue
 * Date: 2019/11/19
 */
public class FollowerMonitorInfo {
    // 当前节点FOLLOWER状态	枚举:
    //CREATED, STARTING, RUNNING, STOPPING, STOPPED, START_FAILED, STOP_FAILED
    private StateServer.ServerState state = null;

    // LEADER节点最大索引序号	当前FOLLOWER节点记录的LEADER节点最大索引序号
    private long leaderMaxIndex = -1;

    public StateServer.ServerState getState() {
        return state;
    }

    public void setState(StateServer.ServerState state) {
        this.state = state;
    }

    public long getLeaderMaxIndex() {
        return leaderMaxIndex;
    }

    public void setLeaderMaxIndex(long leaderMaxIndex) {
        this.leaderMaxIndex = leaderMaxIndex;
    }

    @Override
    public String toString() {
        return "FollowerMonitorInfo{" +
                "state=" + state +
                ", leaderMaxIndex=" + leaderMaxIndex +
                '}';
    }
}
