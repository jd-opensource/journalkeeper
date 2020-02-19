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
package io.journalkeeper.utils.threads;

import io.journalkeeper.utils.state.StateServer;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
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
        if (null == thread) {
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
    public void removeThread(String name) {
        threadMap.remove(name);
    }

    @Override
    public ServerState getTreadState(String name) {
        return getThread(name).serverState();
    }

    @Override
    public boolean exists(String name) {
        return threadMap.containsKey(name);
    }

    @Override
    public synchronized void start() {
        if (serverState == ServerState.STOPPED) {
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
        if (serverState == ServerState.RUNNING) {
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
