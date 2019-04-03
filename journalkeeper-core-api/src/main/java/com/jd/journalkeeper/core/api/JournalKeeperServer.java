package com.jd.journalkeeper.core.api;

import com.jd.journalkeeper.utils.state.StateServer;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

/**
 * Raft节点
 * @param <E> 日志类型
 * @param <Q> 状态查询条件类型
 * @param <R> 状态查询结果类型
 * @author liyue25
 * Date: 2019-03-14
 */
public abstract class JournalKeeperServer<E, Q, R> implements StateServer {

    protected final StateFactory<E, Q, R> stateFactory;
    /**
     * 属性集
     */
    protected final Properties properties;
    public JournalKeeperServer(StateFactory<E, Q, R> stateFactory, Properties properties){
        this.stateFactory = stateFactory;
        this.properties = properties;
    }
    public Properties properties() {
        return properties;
    }
    public abstract Roll roll();
    public abstract void init(URI uri, List<URI> voters) throws IOException;
    public abstract void recover() throws IOException;

    public enum Roll {VOTER, OBSERVER}
}
