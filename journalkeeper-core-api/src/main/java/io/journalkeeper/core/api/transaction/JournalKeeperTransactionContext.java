package io.journalkeeper.core.api.transaction;

import io.journalkeeper.core.api.transaction.TransactionContext;
import io.journalkeeper.core.api.transaction.TransactionId;

import java.util.Map;
import java.util.Objects;

/**
 * @author LiYue
 * Date: 2019/11/29
 */
public class JournalKeeperTransactionContext implements TransactionContext {
    private final UUIDTransactionId transactionId;
    private final Map<String, String> context;
    private final long timestamp;

    public JournalKeeperTransactionContext(UUIDTransactionId transactionId, Map<String, String> context, long timestamp) {
        this.transactionId = transactionId;
        this.context = context;
        this.timestamp = timestamp;
    }

    @Override
    public TransactionId transactionId() {
        return transactionId;
    }

    @Override
    public Map<String, String> context() {
        return context;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JournalKeeperTransactionContext context1 = (JournalKeeperTransactionContext) o;
        return timestamp == context1.timestamp &&
                Objects.equals(transactionId, context1.transactionId) &&
                Objects.equals(context, context1.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, context, timestamp);
    }

    @Override
    public String toString() {
        return "JournalKeeperTransactionContext{" +
                "transactionId=" + transactionId +
                ", context=" + context +
                ", timestamp=" + timestamp +
                '}';
    }
}
