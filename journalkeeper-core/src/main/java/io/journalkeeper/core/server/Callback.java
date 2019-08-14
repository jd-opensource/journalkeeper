package io.journalkeeper.core.server;

import io.journalkeeper.rpc.client.UpdateClusterStateResponse;

import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2019-08-14
 */
class Callback<R> {
    private final long position;
    private final long timestamp;
    private final CompletableFuture<UpdateClusterStateResponse> completableFuture;
    private R result;

    Callback(long position, CompletableFuture<UpdateClusterStateResponse> completableFuture) {
        this.position = position;
        this.timestamp = System.currentTimeMillis();
        this.completableFuture = completableFuture;
    }

    long getPosition() {
        return position;
    }

    R getResult() {
        return result;
    }

    long getTimestamp() {
        return timestamp;
    }

    CompletableFuture<UpdateClusterStateResponse> getCompletableFuture() {
        return completableFuture;
    }

    void setResult(R result) {
        this.result = result;
    }
}
