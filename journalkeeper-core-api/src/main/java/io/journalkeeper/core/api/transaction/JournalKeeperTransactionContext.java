/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.api.transaction;

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
