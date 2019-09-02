/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.utils.threads;


import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 一个后台线程，实现类似：
 * while(true){
 *     doWork();
 * }
 * 的线程。
 */
abstract class LoopThread implements AsyncLoopThread {
    private Thread thread = null;
    private String name;
    protected long minSleep = 50L,maxSleep = 500L;
    private boolean daemon;
    private final Lock wakeupLock = new ReentrantLock();
    private final java.util.concurrent.locks.Condition wakeupCondition = wakeupLock.newCondition();
    private volatile ServerState serverState = ServerState.STOPPED;
    private AtomicBoolean needToWakeUp = new AtomicBoolean(false);
    /**
     * 每次循环需要执行的代码。
     */
    abstract void doWork() throws Throwable;

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean isDaemon() {
        return daemon;
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    /**
     * doWork() 前判断是否满足条件。
     * @return true: 执行doWork。
     */
    protected boolean condition() {
        return true;
    }

    @Override
    public synchronized void start() {
        if(!isStarted()) {
            serverState = ServerState.STARTING;
            thread = new Thread(this);
            thread.setName(name == null ? "LoopThread": name);
            thread.setDaemon(daemon);
            thread.start();
        }
    }

    @Override
    public synchronized void stop() {

        if(serverState != ServerState.STOPPED) {
            serverState = ServerState.STOPPING;
            thread.interrupt();
            while (serverState != ServerState.STOPPED) {
                try {
                    wakeup();
                    Thread.sleep(10L);
                } catch (InterruptedException ignored) {
                }
            }

        }
    }

    private boolean isStarted() {
        return serverState() == ServerState.RUNNING;
    }
    @Override
    public ServerState serverState() {
        return serverState;
    }

    @Override
    public void run() {
        if(serverState == ServerState.STARTING) {
            serverState = ServerState.RUNNING;
        }
        while (serverState == ServerState.RUNNING) {

            long t0 = System.nanoTime();
            try {
                if(condition()) {
                    doWork();
                }
                long t1 = System.nanoTime();

                // 为了避免空转CPU高，如果执行时间过短，等一会儿再进行下一次循环
                if (t1 - t0 < minSleep * 100000L) {

                    wakeupLock.lock();
                    try {
                        needToWakeUp.set(true);
                        wakeupCondition.await(minSleep < maxSleep ? ThreadLocalRandom.current().nextLong(minSleep, maxSleep) : minSleep, TimeUnit.MILLISECONDS);

                    } finally {
                        wakeupLock.unlock();
                    }
                }

            } catch (InterruptedException i) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                if (!handleException(t)) {
                    break;
                }
            }
        }
        serverState = ServerState.STOPPED;
    }

    /**
     * 唤醒任务如果任务在Sleep
     */
    @Override
    public void wakeup() {

        if(needToWakeUp.compareAndSet(true, false)) {
            wakeupLock.lock();
            try {
                wakeupCondition.signal();
            } finally {
                wakeupLock.unlock();
            }
        }
    }

    /**
     * 处理doWork()捕获的异常
     * @return true：继续循环，false：结束线程
     */
    protected boolean handleException(Throwable t) {
        return true;
    }

}
