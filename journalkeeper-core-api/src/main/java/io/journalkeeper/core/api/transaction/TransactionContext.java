package io.journalkeeper.core.api.transaction;

import java.util.Map;

/**
 * Transaction context.
 * The context of a transaction is provided by user while create the transaction,
 * and it will be write into the transaction prepare log.
 *
 * @author LiYue
 * Date: 2019/11/29
 */
public interface TransactionContext {
    TransactionId transactionId();
    Map<String, String> context();
    long timestamp();
}
