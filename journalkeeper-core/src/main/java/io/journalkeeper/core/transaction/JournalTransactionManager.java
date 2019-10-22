package io.journalkeeper.core.transaction;

import io.journalkeeper.core.exception.JournalException;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public class JournalTransactionManager {
    private final Journal journal;
    private final ClientServerRpc server;

    public JournalTransactionManager(Journal journal, ClientServerRpc server) {
        this.journal = journal;
        this.server = server;
    }

    private final TransactionEntrySerializer transactionEntrySerializer = new TransactionEntrySerializer();
    public CompletableFuture<UUID> createTransaction() {
        int partition = selectAFreeTransactionPartition();
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

    private int selectAFreeTransactionPartition() {
        return 0;
    }

    public CompletableFuture<Void> completeTransaction(UUID transactionId, boolean completeOrAbort) {
        int partition = getTransactionPartition(transactionId);
        ensureTransactionOpen(transactionId, partition);
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

    private void ensureTransactionOpen(UUID transactionId, int partition) {

    }

    private int getTransactionPartition(UUID transactionId) {
        return 0;
    }


}
