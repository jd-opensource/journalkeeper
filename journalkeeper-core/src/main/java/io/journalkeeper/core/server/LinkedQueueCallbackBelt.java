package io.journalkeeper.core.server;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;

import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author LiYue
 * Date: 2019-08-14
 */
class LinkedQueueCallbackBelt<ER> implements CallbackBelt<ER> {
    private final ConcurrentLinkedQueue<Callback<ER>> queue = new ConcurrentLinkedQueue<>();
    private final AtomicLong callbackPosition = new AtomicLong(0L);
    private final long timeoutMs;
    private final Serializer<ER> serialize;

    LinkedQueueCallbackBelt(long timeoutMs, Serializer<ER> serialize) {
        this.timeoutMs = timeoutMs;
        this.serialize = serialize;
    }

    private Callback<ER> getFirst() {
        final Callback<ER> f = queue.peek();
        if (f == null)
            throw new NoSuchElementException();
        return f;
    }

    private Callback<ER> removeFirst() {
        final Callback<ER> f = queue.poll();
        if (f == null)
            throw new NoSuchElementException();
        return f;
    }

    @Override
    public int size() {
        return queue.size();
    }

    /**
     * NOT Thread-safe!!!!!!
     */
    public void callbackBefore(long position) {
        callbackPosition.set(position);
        try {
            while (getFirst().getPosition() < position) {
                Callback<ER> callback = removeFirst();

                callback.getCompletableFuture().complete(new UpdateClusterStateResponse(serialize.serialize(callback.getResult())));
            }
//            long deadline = System.currentTimeMillis() - timeoutMs;
//            while (getFirst().getTimestamp() < deadline) {
//                Callback<ER> callback = removeFirst();
//                callback.getCompletableFuture().complete(new UpdateClusterStateResponse(new TimeoutException()));
//            }
        } catch (NoSuchElementException ignored) {
        }
    }
    @Override
    public void put(Callback<ER> callback) {
        queue.add(callback);
    }

    @Override
    public boolean setResult(long position, ER result) {
        Callback<ER> callback = get(position);
        if(callback != null) {
            callback.setResult(result);
            return true;
        } else {
            return false;
        }
    }

    private Callback<ER> get(long position) {
        for (Callback<ER> callback : queue) {
            if(callback.getPosition() == position) {
                return callback;
            } else if (callback.getPosition() > position) {
                break;
            }
        }
        return null;
    }

}
