package com.jd.journalkeeper.journalstore;

import com.jd.journalkeeper.core.api.JournalStore;
import com.jd.journalkeeper.core.api.ResponseConfig;
import com.jd.journalkeeper.core.server.Server;
import com.jd.journalkeeper.rpc.client.QueryStateRequest;
import com.jd.journalkeeper.rpc.client.UpdateClusterStateRequest;
import com.jd.journalkeeper.utils.event.EventWatcher;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public class JournalStoreServer implements JournalStore {
    private final Server<byte[], JournalStoreQuery, List<byte[]>> raftServer;
    private final ExecutorService asyncExecutor;
    private final JournalStoreEntrySerializer entrySerializer;
    private final JournalStoreQuery querySerializer;

    public JournalStoreServer(Server<byte[], JournalStoreQuery, List<byte[]>> raftServer) {
        this(raftServer, ForkJoinPool.commonPool());
    }

    public JournalStoreServer(Server<byte[], JournalStoreQuery, List<byte[]>> raftServer, ExecutorService asyncExecutor) {
        this.raftServer = raftServer;
        this.asyncExecutor = asyncExecutor;
        this.entrySerializer = new JournalStoreEntrySerializer();
        this.querySerializer = new JournalStoreQuery();
    }

    @Override
    public CompletableFuture<Void> append(List<byte[]> entries) {
        return append(entries, ResponseConfig.REPLICATION);
    }

    @Override
    public CompletableFuture<Void> append(List<byte[]> entries, ResponseConfig responseConfig) {
        return raftServer
                .updateClusterState(new UpdateClusterStateRequest(entrySerializer.serialize(entries), responseConfig))
                .thenAccept(response -> {
                    if(!response.success()){
                        throw new AppendEntryException(response.getError());
                    }
                });    }

    @Override
    public CompletableFuture<List<byte[]>> get(long index, int size) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return raftServer
                        .queryServerState(new QueryStateRequest(querySerializer.serialize(new JournalStoreQuery(index, size))))
                        .thenApply(response -> {
                            if(!response.success()){
                                throw new GetEntryException(response.getError());
                            } else {
                                return response.getResult();
                            }
                        })
                        .thenApply(entrySerializer::parse).get();
            } catch (Throwable throwable) {
                throw new GetEntryException(throwable);
            }
        });
    }

    @Override
    public CompletableFuture<Long> minIndex() {
        return CompletableFuture.supplyAsync(() -> ((JournalStoreState) raftServer.getState()).minIndex());
    }

    @Override
    public CompletableFuture<Long> maxIndex() {
        return CompletableFuture.supplyAsync(() -> ((JournalStoreState) raftServer.getState()).maxIndex());
    }

    @Override
    public CompletableFuture<Void> compact(long minIndexExclusive) {
        return CompletableFuture.runAsync(() -> {
            try {
                raftServer.compact(minIndexExclusive);
                JournalStoreState state = (JournalStoreState) raftServer.getState();
                state.compact(minIndexExclusive);

            } catch (Throwable throwable) {
                throw new CompletionException(throwable);
            }
        }, asyncExecutor);
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        raftServer.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        raftServer.unWatch(eventWatcher);
    }
}
