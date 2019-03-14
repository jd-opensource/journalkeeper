package io.journalkeeper.base;

import java.util.concurrent.CompletableFuture;

/**
 * 有状态的服务
 * @author liyue25
 * Date: 2019-03-14
 */
public interface StateServer {
    CompletableFuture<Void> start();
    CompletableFuture<Void> stop();
    ServerState status();

    enum ServerState {STOPPED, STARTING, RUNNING, STOPPING}
}
