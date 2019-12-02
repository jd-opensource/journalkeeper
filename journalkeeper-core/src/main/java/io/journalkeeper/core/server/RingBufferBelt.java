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
package io.journalkeeper.core.server;

import io.journalkeeper.utils.buffer.LockFreeRingBuffer;

import java.util.concurrent.TimeoutException;

/**
 * @author LiYue
 * Date: 2019-09-19
 */
public class RingBufferBelt implements CallbackResultBelt {

    private final long timeoutMs;
    private final LockFreeRingBuffer<Callback> buffer;
    RingBufferBelt(long timeoutMs, int capacity) {
        this.timeoutMs = timeoutMs;
        buffer = new LockFreeRingBuffer<>(Callback.class, capacity);
    }


    @Override
    public boolean full() {
        return buffer.full();
    }

    @Override
    public void put(Callback callback) throws InterruptedException {
        while (!buffer.put(callback)) {
            Thread.sleep(1);
        }
    }

    @Override
    public void callbackBefore(long position) {
        Callback c;

        while ((c = buffer.get()) != null && c.getPosition() <= position) {
            c = buffer.remove();
            if(null != c) {
                c.getResponseFuture().countDownFlush();
            }
        }
        callbackTimeouted();
    }

    private void callbackTimeouted() {
        long deadline = System.currentTimeMillis() - timeoutMs;
        Callback c;
        while ((c = buffer.get()) != null && c.getTimestamp() < deadline) {
            c = buffer.remove();
            if(null != c) {
                c.getResponseFuture().completedExceptionally(new TimeoutException());
            }
        }
    }

    @Override
    public void callback(long position, byte [] result) {

        Callback c = buffer.get();
        if (null != c && c.getPosition() == position) {
            c = buffer.remove();
            c.getResponseFuture().putResult(result);

        }
    }

    @Override
    public void failAll() {
        while (!buffer.empty()){
            buffer.remove()
                    .getResponseFuture().completedExceptionally(new IllegalStateException());
        }
    }
}
