package io.journalkeeper.rpc.client;

import io.journalkeeper.rpc.LeaderResponse;

import java.util.UUID;

/**
 * 创建事务的响应
 */
public class CreateTransactionResponse extends LeaderResponse {
    private final UUID transactionId;

    public CreateTransactionResponse(Throwable throwable){
        this(throwable, null);
    }

    public CreateTransactionResponse(UUID transactionId) {
        this(null, transactionId);
    }

    private CreateTransactionResponse(Throwable exception, UUID transactionId) {
        super(exception);
        this.transactionId = transactionId;
    }

    /**
     * 新创建的事务ID
     * @return 事务ID
     */
    public UUID getTransactionId() {
        return transactionId;
    }
}
