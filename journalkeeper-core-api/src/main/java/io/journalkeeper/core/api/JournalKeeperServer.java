package io.journalkeeper.core.api;

import io.journalkeeper.base.Replicable;
import io.journalkeeper.base.StateServer;

import java.net.URI;
import java.util.Properties;
import java.util.Set;

/**
 * Raft节点
 * @author liyue25
 * Date: 2019-03-14
 */
public abstract class JournalKeeperServer<E,  S extends Replicable<S>> implements ClusterAccessPoint, StateServer {

    protected Set<URI> voters;
    protected final StateMachine<E, S> stateMachine;
    protected final Properties properties;
    protected final URI uri;
    JournalKeeperServer(URI uri, Set<URI> voters, StateMachine<E, S> stateMachine){
        this(uri, voters, stateMachine, new Properties());
    }

    JournalKeeperServer(URI uri, Set<URI> voters, StateMachine<E, S> stateMachine, Properties properties){
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

    public abstract <Q,R> JournalKeeperClient<Q, R, E> getClient();


    public enum Roll {VOTER, OBSERVER}
}
