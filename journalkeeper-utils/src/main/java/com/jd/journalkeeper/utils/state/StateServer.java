package com.jd.journalkeeper.utils.state;


import java.util.concurrent.CompletableFuture;

/**
 * 有状态的服务
 * @author liyue25
 * Date: 2019-03-14
 */
public interface StateServer {
    void start();
    void stop();
    ServerState serverState();

    enum ServerState {STOPPED, STARTING, RUNNING, STOPPING}

    default CompletableFuture<Void> startAsync(){
        return CompletableFuture.runAsync(this::start);
    }

    default CompletableFuture<Void> stopAsync(){
        return CompletableFuture.runAsync(this::stop);
    }
}
