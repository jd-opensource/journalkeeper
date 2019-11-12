package io.journalkeeper.core.transaction;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.api.JournalEntryParser;
import io.journalkeeper.core.exception.JournalException;
import io.journalkeeper.core.exception.TransactionException;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.utils.state.ServerStateMachine;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public class JournalTransactionManager extends ServerStateMachine {
    private final ClientServerRpc server;
    private final JournalTransactionState transactionState;
    private final Map<UUID, CompletableFuture<Void>> pendingCompleteTransactionFutures = new ConcurrentHashMap<>();
    public JournalTransactionManager(Journal journal, ClientServerRpc server,
                                     ScheduledExecutorService scheduledExecutor, long transactionTimeoutMs) {
        this.server = server;
        this.transactionState = new JournalTransactionState(journal, transactionTimeoutMs, server, scheduledExecutor);
    }

    @Override
    protected void doStart() {
        super.doStart();
        this.transactionState.start();
    }

    @Override
    protected void doStop() {
        this.transactionState.stop();
        super.doStop();
    }

    private final TransactionEntrySerializer transactionEntrySerializer = new TransactionEntrySerializer();

    public CompletableFuture<UUID> createTransaction() {
        int partition = transactionState.nextFreePartition();
        UUID transactionId = UUID.randomUUID();
        TransactionEntry entry = new TransactionEntry(transactionId);
        byte [] serializedEntry = transactionEntrySerializer.serialize(entry);

        return server.updateClusterState(new UpdateClusterStateRequest(serializedEntry, partition, 1))
                .thenApply(response -> {
                    if(response.success()) {
                        return transactionId;
                    } else {
                        throw new JournalException(response.errorString());
                    }
                });
    }

    public CompletableFuture<Void> completeTransaction(UUID transactionId, boolean completeOrAbort) {
        int partition = getTransactionPartition(transactionId);
        ensureTransactionOpen(transactionId);
        TransactionEntry entry = new TransactionEntry(transactionId, TransactionEntryType.TRANSACTION_PRE_COMPLETE, completeOrAbort);
        byte [] serializedEntry = transactionEntrySerializer.serialize(entry);
        CompletableFuture<Void> future = new CompletableFuture<>();
        pendingCompleteTransactionFutures.put(transactionId, future);
        server
                .updateClusterState(new UpdateClusterStateRequest(serializedEntry, partition, 1))
                .thenAccept(response -> {
                    if(!response.success()) {
                        CompletableFuture<Void> retFuture = pendingCompleteTransactionFutures.remove(transactionId);
                        if(null != retFuture) {
                            retFuture.completeExceptionally(new TransactionException(response.errorString()));
                        }
                    }
                });
        return future;
    }

    public JournalEntry wrapTransactionalEntry(JournalEntry entry, UUID transactionId, JournalEntryParser journalEntryParser) {
        return transactionState.wrapTransactionalEntry(entry, transactionId, journalEntryParser);
    }

    private void ensureTransactionOpen(UUID transactionId) {
        transactionState.ensureTransactionOpen(transactionId);
    }

    private int getTransactionPartition(UUID transactionId) {
        return transactionState.getPartition(transactionId);
    }

    public Collection<UUID> getOpeningTransactions() {
        return transactionState.getOpeningTransactions();
    }

    public void applyEntry(JournalEntry journalEntry) {
        int partition = journalEntry.getPartition();
        if(transactionState.isTransactionPartition(partition)) {
            TransactionEntry transactionEntry = transactionEntrySerializer.parse(journalEntry.getPayload().getBytes());
            transactionState.applyEntry(transactionEntry, partition, pendingCompleteTransactionFutures);
        }
    }
}
