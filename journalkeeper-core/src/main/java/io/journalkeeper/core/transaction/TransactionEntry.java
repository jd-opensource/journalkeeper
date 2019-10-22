package io.journalkeeper.core.transaction;

import java.util.UUID;

/**
 * @author LiYue
 * Date: 2019/10/22
 */
public class TransactionEntry {
    private final TransactionEntryType type;
    private final UUID transactionId;
    private int partition = -1;
    private boolean commitOrAbort = false;
    private byte [] entry = new byte[0];

    public TransactionEntry(UUID transactionId) {
        this.transactionId = transactionId;
        this.type = TransactionEntryType.TRANSACTION_START;
    }

    public TransactionEntry(UUID transactionId,TransactionEntryType type, boolean commitOrAbort) {
        this.type = type;
        this.transactionId = transactionId;
        this.commitOrAbort = commitOrAbort;
    }

    public TransactionEntry(UUID transactionId, int partition, byte [] entry) {
        this.transactionId = transactionId;
        this.type = TransactionEntryType.TRANSACTION_ENTRY;
        this.partition = partition;
        this.entry = entry;
    }

    public TransactionEntry(UUID transactionId, TransactionEntryType type, int partition, boolean commitOrAbort, byte [] entry) {
        this.transactionId = transactionId;
        this.type = type;
        this.partition = partition;
        this.commitOrAbort = commitOrAbort;
        this.entry = entry;
    }


    public TransactionEntryType getType() {
        return type;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public int getPartition() {
        return partition;
    }

    public boolean isCommitOrAbort() {
        return commitOrAbort;
    }

    public byte[] getEntry() {
        return entry;
    }
}
