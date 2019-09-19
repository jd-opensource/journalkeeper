package io.journalkeeper.core.server;

import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
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
            Thread.yield();
        }
    }

    @Override
    public void callbackBefore(long position) {
        Callback c;

        while ((c = buffer.get()) != null && c.getPosition() <= position) {
            c = buffer.remove();
            if(null != c) {
                c.getCompletableFuture().complete(new UpdateClusterStateResponse(new byte [0]));
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
                c.getCompletableFuture().complete(new UpdateClusterStateResponse(new TimeoutException()));
            }
        }
    }
    @Override
    public void callback(long position, byte [] result) throws InterruptedException {

        Callback c ;
        while ((c = buffer.get()) == null || c.getPosition() < position) {
            Thread.yield();
        }
        if (c.getPosition() == position) {
            c = buffer.remove();
            c.getCompletableFuture().complete(
                    new UpdateClusterStateResponse(result));

        }
    }

    @Override
    public void failAll() {
        while (!buffer.empty()){
            buffer.remove()
                    .getCompletableFuture()
                    .complete(new UpdateClusterStateResponse(new IllegalStateException()));
        }
    }
}
