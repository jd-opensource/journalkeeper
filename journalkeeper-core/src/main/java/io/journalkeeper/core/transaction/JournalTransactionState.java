package io.journalkeeper.core.transaction;

import io.journalkeeper.core.journal.Journal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 事务状态，非持久化
 * @author LiYue
 * Date: 2019/10/22
 */
class JournalTransactionState {
    private final Journal journal;
    private final Collection<Integer> transactionPartitions;
    private final Map<Integer /* partition */, TransactionEntryType /* last transaction entry type */> partitionStausMap;
    private final Map<UUID /* transaction id */, Integer /* partition */> openingTransactionMap ;
    private static final Logger logger = LoggerFactory.getLogger(JournalTransactionState.class);


    JournalTransactionState(Journal journal, Collection<Integer> transactionPartitions) {
        this.journal = journal;
        this.transactionPartitions = Collections.unmodifiableCollection(transactionPartitions);
        this.partitionStausMap = new HashMap<>(this.transactionPartitions.size());
        this.openingTransactionMap = new HashMap<>(this.transactionPartitions.size());
        recoverTransactionState();
    }

    private void recoverTransactionState() {

    }


    void execute(TransactionEntry entry, int partition, long index) {
        if(!transactionPartitions.contains(partition)) {
            logger.warn("Ignore transaction entry, cause: partition {} is not a transaction partition.", partition);
            return;
        }

        partitionStausMap.put(partition, entry.getType());

        switch (entry.getType()) {
            case TRANSACTION_START:
                openingTransactionMap.put(entry.getTransactionId(), partition);
                break;
            case TRANSACTION_PRE_COMPLETE:
                completeTransaction(entry.getTransactionId(), entry.isCommitOrAbort());
                break;
            case TRANSACTION_COMPLETE:
                openingTransactionMap.remove(entry.getTransactionId());
                partitionStausMap.remove(partition);
                break;
        }



    }

    private void completeTransaction(UUID transactionId, boolean commitOrAbort) {

    }

    List<UUID> getOpeningTransactions() {
        return new ArrayList<>(openingTransactionMap.keySet());
    }

    int getPartition(UUID transactionId) {
        return openingTransactionMap.getOrDefault(transactionId, -1);
    }

    void ensureTransactionOpen(UUID transactionId) {
        if(!openingTransactionMap.containsKey(transactionId)) {
            throw new IllegalStateException(
                    String.format("Transaction %s is not open!", transactionId.toString())
            );
        }
    }


}
