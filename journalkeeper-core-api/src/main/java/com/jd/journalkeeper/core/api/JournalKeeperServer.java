package com.jd.journalkeeper.core.api;

import com.jd.journalkeeper.base.Queryable;
import com.jd.journalkeeper.base.Replicable;
import com.jd.journalkeeper.utils.state.StateServer;

import java.net.URI;
import java.util.Properties;
import java.util.Set;

/**
 * Raft节点
 * @param <E> 日志类型
 * @param <S> 状态类型
 * @param <Q> 状态查询条件类型
 * @param <R> 状态查询结果类型
 * @author liyue25
 * Date: 2019-03-14
 */
public abstract class JournalKeeperServer<E,  S extends Replicable<S> & Queryable<Q, R>, Q, R> implements ClusterAccessPoint<Q, R, E>, StateServer {

    /**
     * 所有选民节点地址，包含LEADER
     */
    protected Set<URI> voters;
    /**
     * 状态机
     */
    protected final StateMachine<E, S> stateMachine;
    /**
     * 属性集
     */
    protected final Properties properties;
    /**
     * 当前Server URI
     */
    protected final URI uri;
    public JournalKeeperServer(URI uri, Set<URI> voters, StateMachine<E, S> stateMachine){
        this(uri, voters, stateMachine, new Properties());
    }

    public JournalKeeperServer(URI uri, Set<URI> voters, StateMachine<E, S> stateMachine, Properties properties){
        this.uri = uri;
        this.voters = voters;
        this.stateMachine = stateMachine;
        this.properties = properties;
    }

    public Properties properties() {
        return properties;
    }

    public Set<URI> getVoters() {
        return voters;
    }

    public void setVoters(Set<URI> voters) {
        this.voters = voters;
    }
    public URI uri() {return this.uri;}
    public abstract Roll roll();

    public abstract void recover();

    public enum Roll {VOTER, OBSERVER}
}
