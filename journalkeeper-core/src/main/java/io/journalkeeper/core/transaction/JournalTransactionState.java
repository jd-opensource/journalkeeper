package io.journalkeeper.core.transaction;

import io.journalkeeper.core.api.JournalEntry;
import io.journalkeeper.core.exception.TransactionException;
import io.journalkeeper.core.journal.Journal;
import io.journalkeeper.exceptions.IndexUnderflowException;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 事务状态，非持久化
 * @author LiYue
 * Date: 2019/10/22
 */
class JournalTransactionState {
    private final Journal journal;
    private final Map<Integer /* partition */, TransactionEntryType /* last transaction entry type */> partitionStatusMap;
    private final Map<UUID /* transaction id */, Integer /* partition */> openingTransactionMap ;
    private static final int TRANSACTION_PARTITION_START = 30000;
    private static final int TRANSACTION_PARTITION_COUNT = 128;
    private final ClientServerRpc server;
    private static final Logger logger = LoggerFactory.getLogger(JournalTransactionState.class);
    private final TransactionEntrySerializer transactionEntrySerializer = new TransactionEntrySerializer();
    private final AtomicInteger nextFreePartition = new AtomicInteger(TRANSACTION_PARTITION_START);

    JournalTransactionState(Journal journal,  ClientServerRpc server) {
        this.journal = journal;
        this.server = server;
        this.partitionStatusMap = new HashMap<>(TRANSACTION_PARTITION_COUNT);
        this.openingTransactionMap = new HashMap<>(TRANSACTION_PARTITION_COUNT);
        recoverTransactionState();
    }

    private void recoverTransactionState() {
        for (int i = 0; i < TRANSACTION_PARTITION_COUNT; i++) {
            int partition = TRANSACTION_PARTITION_START + i;
            try {
                JournalEntry journalEntry = journal.readByPartition(partition, journal.maxIndex(partition) - 1);
                TransactionEntry transactionEntry = transactionEntrySerializer.parse(journalEntry.getPayload().getBytes());
                partitionStatusMap.put(partition, transactionEntry.getType());
                if(transactionEntry.getType() != TransactionEntryType.TRANSACTION_COMPLETE) {
                    openingTransactionMap.put(transactionEntry.getTransactionId(), partition);
                }



            } catch (IndexUnderflowException ignored) {}
        }
    }


    int nextFreePartition() {
        int partition = nextPartition();
        int start = partition;
        synchronized (partitionStatusMap) {
            while (partitionStatusMap.getOrDefault(partition, TransactionEntryType.TRANSACTION_COMPLETE) != TransactionEntryType.TRANSACTION_COMPLETE) {
                partition = nextPartition();
                if(partition == start) {
                    throw new TransactionException("No free transaction partition!");
                }
            }
            partitionStatusMap.put(partition, TransactionEntryType.TRANSACTION_PRE_START);
            return partition;
        }
    }


    private int nextPartition() {
        int partition = nextFreePartition.getAndIncrement();
        nextFreePartition.compareAndSet(
                TRANSACTION_PARTITION_START + TRANSACTION_PARTITION_COUNT,
                TRANSACTION_PARTITION_START
        );

        return partition;
    }
    void execute(TransactionEntry entry, int partition, long index) throws ExecutionException, InterruptedException {
        if(partition < TRANSACTION_PARTITION_START || partition >= TRANSACTION_PARTITION_START + TRANSACTION_PARTITION_COUNT) {
            logger.warn("Ignore transaction entry, cause: partition {} is not a transaction partition.", partition);
            return;
        }

        partitionStatusMap.put(partition, entry.getType());

        switch (entry.getType()) {
            case TRANSACTION_START:
                openingTransactionMap.put(entry.getTransactionId(), partition);
                break;
            case TRANSACTION_PRE_COMPLETE:
                completeTransaction(entry.getTransactionId(), entry.isCommitOrAbort(), partition, index);
                break;
            case TRANSACTION_COMPLETE:
                openingTransactionMap.remove(entry.getTransactionId());
                partitionStatusMap.remove(partition);
                break;
        }



    }

    /**
     * 执行状态机阶段，针对“TRANSACTION_PRE_COMPLETE”的日志，需要执行：
     *
     * 1. 如果操作是回滚，直接写入TRANSACTION_COMPLETE日志；
     * 2. 如果操作是提交：
     * 3. 这个事务中所有消息涉及到的每个分区：读出该分区的所有事务日志，组合成一个BatchEntry写入对应分区。
     * 4. 上步骤中每条日志都写入成功后，写入TRANSACTION_COMPLETE日志，提交成功。
     *
     * @param transactionId 事务ID。
     * @param commitOrAbort true：提条事务，false：回滚事务。
     * @param partition 事务分区。
     * @param index 索引序号
     */
    private void completeTransaction(UUID transactionId, boolean commitOrAbort, int partition, long index) throws ExecutionException, InterruptedException {

        if(commitOrAbort) {
            TransactionEntry transactionEntry;
            long i = index;
            Map<Integer /* partition */, List<TransactionEntry> /* transaction entries */> transactionEntriesByPartition
                    = new HashMap<>();
            do {
                JournalEntry journalEntry = journal.readByPartition(partition, --i);
                transactionEntry = transactionEntrySerializer.parse(journalEntry.getPayload().getBytes());

                if(!transactionId.equals(transactionEntry.getTransactionId()) ||
                        transactionEntry.getType() == TransactionEntryType.TRANSACTION_START) {
                    break;
                }

                if(transactionEntry.getType() == TransactionEntryType.TRANSACTION_ENTRY) {
                    List<TransactionEntry> list =
                            transactionEntriesByPartition.computeIfAbsent(transactionEntry.getPartition(), p -> new LinkedList<>());
                    list.add(transactionEntry);
                }

            } while (i > 0);

            for (Map.Entry<Integer, List<TransactionEntry>> me : transactionEntriesByPartition.entrySet()) {
                int bizPartition = me.getKey();
                List<TransactionEntry> transactionEntries = me.getValue();

                for (TransactionEntry te : transactionEntries) {
                    server.updateClusterState(
                            new UpdateClusterStateRequest(
                                    te.getEntry(), bizPartition, te.getBatchSize()
                            )
                    ).get();
                }
            }
        }

        writeTransactionCompleteEntry(transactionId, commitOrAbort, partition);

    }

    private void writeTransactionCompleteEntry(UUID transactionId, boolean commitOrAbort, int partition) throws ExecutionException, InterruptedException {
        TransactionEntry entry = new TransactionEntry(transactionId, TransactionEntryType.TRANSACTION_COMPLETE, commitOrAbort);
        byte [] serializedEntry = transactionEntrySerializer.serialize(entry);
        server.updateClusterState(new UpdateClusterStateRequest(
                serializedEntry, partition, 1
        )).get();
    }

    Collection<UUID> getOpeningTransactions() {
        return Collections.unmodifiableCollection(openingTransactionMap.keySet());
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
