package com.jd.journalkeeper.utils.threads;

import com.jd.journalkeeper.utils.state.StateServer;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * @author liyue25
 * Date: 2019-06-21
 */
public class ThreadsManager implements Threads {

    private final Map<String, AsyncLoopThread> threadMap;
    private ServerState serverState = ServerState.STOPPED;

    public ThreadsManager() {
        this.threadMap = new HashMap<>();
    }


    @Override
    public void createThread(AsyncLoopThread asyncThread) {
        if (null != threadMap.putIfAbsent(asyncThread.getName(), asyncThread)) {
            throw new IllegalStateException(String.format("Thread name \"%s\" already exists.", asyncThread.getName()));
        }
    }

    @Override
    public void wakeupThread(String name) {
        getThread(name).wakeup();
    }

    private AsyncLoopThread getThread(String name) {
            AsyncLoopThread thread = threadMap.get(name);
            if(null == thread) {
                throw new NoSuchElementException(String.format("Thread name \"%s\" NOT exists.", name));
            }
            return thread;
    }

    @Override
    public void stopThread(String name) {
        getThread(name).stop();
    }

    @Override
    public void startThread(String name) {
        getThread(name).start();
    }

    @Override
    public ServerState getTreadState(String name) {
        return getThread(name).serverState();
    }

    @Override
    public synchronized void start() {
        if(serverState == ServerState.STOPPED) {
            serverState = ServerState.STARTING;
            CompletableFuture.allOf(
                    threadMap.values().stream()
                            .filter(t -> t.serverState() == ServerState.STOPPED)
                            .map(StateServer::startAsync)
                            .toArray(CompletableFuture[]::new))
                    .join();
            serverState = ServerState.RUNNING;
        } else {
            throw new IllegalStateException();
        }

    }

    @Override
    public synchronized void stop() {
        if(serverState == ServerState.RUNNING) {
            serverState = ServerState.STOPPING;
            CompletableFuture.allOf(
                    threadMap.values().stream()
                            .filter(t -> t.serverState() != ServerState.STOPPED)
                            .map(StateServer::stopAsync)
                            .toArray(CompletableFuture[]::new))
                    .join();
            serverState = ServerState.STOPPED;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public ServerState serverState() {
        return serverState;
    }
}
