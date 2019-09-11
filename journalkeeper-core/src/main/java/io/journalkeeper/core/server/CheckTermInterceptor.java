package io.journalkeeper.core.server;

import io.journalkeeper.rpc.server.Termed;

/**
 * 检查Raft RPC请求和响应中的Term，如果发现对方的term更高，则转为follower
 * @author LiYue
 * Date: 2019-09-11
 */
interface CheckTermInterceptor {
    /**
     * 检查Raft RPC请求和响应中的Term，如果发现对方的term更高，则转为follower
     * @param term 请求或响应中的term
     * @return true：请求响应中的term大于当前term，否则返回false
     */
    boolean checkTerm(int term);
}
