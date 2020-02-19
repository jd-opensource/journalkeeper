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
package io.journalkeeper.utils.state;

/**
 * @author LiYue
 * Date: 2019-09-10
 */
public class ServerStateMachine implements StateServer {
    // 一次性的
    private final boolean oneTime;
    private ServerState state = ServerState.CREATED;
    private Runnable startRunnable = null;
    private Runnable stopRunnable = null;

    public ServerStateMachine() {
        this(false);
    }

    public ServerStateMachine(boolean oneTime) {
        this.oneTime = oneTime;
    }


    @Override
    public final synchronized void start() {
        if (oneTime && state != ServerState.CREATED) {
            throw new IllegalStateException(
                    String.format("Server state should be CREATED! state not changed, current state: %s.",
                            state.toString()));
        }

        if (state != ServerState.CREATED && state != ServerState.STOPPED) {
            throw new IllegalStateException(
                    String.format("Server state should be CREATED or STOPPED! state not changed, current state: %s.",
                            state.toString()));
        }

        state = ServerState.STARTING;
        try {
            doStart();
            state = ServerState.RUNNING;
        } catch (Throwable t) {
            state = ServerState.START_FAILED;
            throw t;
        }

    }

    public final void start(Runnable startRunnable) {
        this.startRunnable = startRunnable;
        start();
    }

    public final void stop(Runnable stopRunnable) {
        this.stopRunnable = stopRunnable;
        start();
    }

    protected void doStart() {
        if (null != startRunnable) {
            startRunnable.run();
        }
    }

    @Override
    public final synchronized void stop() {
        if (state != ServerState.RUNNING) {
            throw new IllegalStateException(
                    String.format("Server state should be RUNNING! state not changed, current state: %s.",
                            state.toString()));
        }

        state = ServerState.STOPPING;
        try {
            doStop();
            state = ServerState.STOPPED;
        } catch (Throwable t) {
            state = ServerState.STOP_FAILED;
            throw t;
        }

    }

    protected void doStop() {
        if (null != stopRunnable) {
            stopRunnable.run();
        }
    }

    public final synchronized void stopQuiet() {
        try {
            stop();
        } catch (Throwable t) {
            try {
                resetFailedState();
            } catch (Throwable tr) {
                state = ServerState.STOPPED;
            }
        }
    }

    public final void onStart(Runnable startRunnable) {
        this.startRunnable = startRunnable;
    }

    public final void onStop(Runnable stopRunnable) {
        this.stopRunnable = stopRunnable;
    }

    public final void resetFailedState() {
        resetFailedState(null);
    }

    public final synchronized void resetFailedState(Runnable resetRunnable) {
        if (state != ServerState.START_FAILED && state != ServerState.STOP_FAILED) {
            throw new IllegalStateException(
                    String.format("Server state should be START_FAILED or STOP_FAILED! state not changed, current state: %s.",
                            state.toString()));
        }
        if (null != resetRunnable) {
            resetRunnable.run();
        }
        state = ServerState.STOPPED;
    }

    @Override
    public ServerState serverState() {
        return state;
    }
}
