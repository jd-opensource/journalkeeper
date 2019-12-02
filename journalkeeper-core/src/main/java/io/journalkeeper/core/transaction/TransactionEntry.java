/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.transaction;

import java.util.Map;
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
    private int batchSize = 1;
    private long timestamp = System.currentTimeMillis() ;
    private Map<String, String> context;

    public TransactionEntry(UUID transactionId, Map<String, String> context) {
        this.transactionId = transactionId;
        this.context = context;
        this.type = TransactionEntryType.TRANSACTION_START;

    }

    public TransactionEntry(UUID transactionId,TransactionEntryType type, boolean commitOrAbort) {
        this.type = type;
        this.transactionId = transactionId;
        this.commitOrAbort = commitOrAbort;
    }

    public TransactionEntry(UUID transactionId, int partition, int batchSize, byte [] entry) {
        this.transactionId = transactionId;
        this.type = TransactionEntryType.TRANSACTION_ENTRY;
        this.partition = partition;
        this.batchSize = batchSize;
        this.entry = entry;
    }

    public TransactionEntry(UUID transactionId, long timestamp, TransactionEntryType type, int partition, boolean commitOrAbort, int batchSize, byte [] entry, Map<String, String> context) {
        this.transactionId = transactionId;
        this.timestamp = timestamp;
        this.type = type;
        this.partition = partition;
        this.commitOrAbort = commitOrAbort;
        this.batchSize = batchSize;
        this.entry = entry;
        this.context = context;
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

    public int getBatchSize() {
        return batchSize;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, String> getContext() {
        return context;
    }
}
