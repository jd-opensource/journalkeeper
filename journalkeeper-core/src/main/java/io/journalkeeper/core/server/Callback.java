package io.journalkeeper.core.server;

import io.journalkeeper.rpc.client.UpdateClusterStateResponse;

import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2019-08-14
 */
class Callback {
    private final long position;
    private final long timestamp;
    private final CompletableFuture<UpdateClusterStateResponse> completableFuture;

    Callback(long position, CompletableFuture<UpdateClusterStateResponse> completableFuture) {
        this.position = position;
        this.timestamp = System.currentTimeMillis();
        this.completableFuture = completableFuture;
    }

    long getPosition() {
        return position;
    }


    long getTimestamp() {
        return timestamp;
    }

    CompletableFuture<UpdateClusterStateResponse> getCompletableFuture() {
        return completableFuture;
    }

}
