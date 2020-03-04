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
package io.journalkeeper.core.server;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
class ThreadNames {
    /**
     * 每个Server模块中需要运行一个用于执行日志更新状态，保存Snapshot的状态机线程，
     */
    final static String STATE_MACHINE_THREAD = "StateMachineThread";
    /**
     * 刷盘Journal线程
     */
    final static String FLUSH_JOURNAL_THREAD = "FlushJournalThread";
    /**
     * 打印Metric线程
     */
    final static String PRINT_METRIC_THREAD = "PrintMetricThread";

    /**
     * Leader接收客户端请求串行写入entries线程
     */
    static final String LEADER_APPEND_ENTRY_THREAD = "LeaderAppendEntryThread";
    /**
     * Voter 处理AppendEntriesRequest线程
     */
    static final String VOTER_REPLICATION_REQUESTS_HANDLER_THREAD = "VoterReplicationRequestHandlerThread";
    /**
     * Voter 处理回调线程
     */
    static final String LEADER_CALLBACK_THREAD = "LeaderCallbackThread";
    /**
     * Leader 发送AppendEntries RPC线程
     */
    static final String LEADER_REPLICATION_THREAD = "LeaderReplicationThread";
    /**
     * Leader 提交线程
     */
    static final String LEADER_COMMIT_THREAD = "leaderCommitThread";

    /**
     * Observer 从其它节点拉取消息线程
     */
    static final String OBSERVER_REPLICATION_THREAD = "ObserverReplicationThread";

}
