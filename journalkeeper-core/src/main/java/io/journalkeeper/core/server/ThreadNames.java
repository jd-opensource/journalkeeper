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
     * Leader 处理AppendEntries RPC Response 线程
     */
    static final String LEADER_REPLICATION_RESPONSES_HANDLER_THREAD = "LeaderReplicationResponsesHandlerThread";

    /**
     * Observer 从其它节点拉取消息线程
     */
    static final String OBSERVER_REPLICATION_THREAD = "ObserverReplicationThread";

}
