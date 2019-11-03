package io.journalkeeper.rpc.client;

import java.util.UUID;

public class CompleteTransactionRequest {
    private final UUID transactionId;
    private final boolean commitOrAbort;

    public CompleteTransactionRequest(UUID transactionId, boolean commitOrAbort) {
        this.transactionId = transactionId;
        this.commitOrAbort = commitOrAbort;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public boolean isCommitOrAbort() {
        return commitOrAbort;
    }
}
