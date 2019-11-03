package io.journalkeeper.rpc.client;

import io.journalkeeper.rpc.LeaderResponse;

import java.util.Collection;
import java.util.UUID;

/**
 * 创建事务的响应
 */
public class GetOpeningTransactionsResponse extends LeaderResponse {
    private final Collection<UUID> transactionIds;

    public GetOpeningTransactionsResponse(Throwable throwable){
        this(throwable, null);
    }

    public GetOpeningTransactionsResponse(Collection<UUID> transactionIds) {
        this(null, transactionIds);
    }

    private GetOpeningTransactionsResponse(Throwable exception, Collection<UUID> transactionIds) {
        super(exception);
        this.transactionIds = transactionIds;
    }

    /**
     * 新创建的事务ID
     * @return 事务ID
     */
    public Collection<UUID> getTransactionIds() {
        return transactionIds;
    }
}
