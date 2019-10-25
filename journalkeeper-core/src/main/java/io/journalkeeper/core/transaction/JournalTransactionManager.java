package io.journalkeeper.core.transaction;

import io.journalkeeper.core.exception.JournalException;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public class JournalTransactionManager {
    private final ClientServerRpc server;
    private final JournalTransactionState transactionState;

    public JournalTransactionManager(Journal journal, ClientServerRpc server) {
        this.server = server;
        this.transactionState = new JournalTransactionState(journal, server);
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
        return server.updateClusterState(new UpdateClusterStateRequest(serializedEntry, partition, 1))
                .thenApply(response -> {
                    if(response.success()) {
                        return null;
                    } else {
                        throw new JournalException(response.errorString());
                    }
                });
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

    public void execute(TransactionEntry entry, int partition, long index) throws ExecutionException, InterruptedException {
        transactionState.execute(entry, partition, index);
    }
}
