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
package io.journalkeeper.rpc.client;

import io.journalkeeper.core.api.transaction.TransactionId;
import io.journalkeeper.core.api.transaction.UUIDTransactionId;
import io.journalkeeper.rpc.LeaderResponse;

import java.util.Map;
import java.util.UUID;

/**
 * 创建事务的响应
 */
public class CreateTransactionResponse extends LeaderResponse {
    private final UUIDTransactionId transactionId;
    private final long timestamp;
    public CreateTransactionResponse(Throwable throwable){
        this(throwable, null, -1L);
    }

    public CreateTransactionResponse(UUIDTransactionId transactionId, long timestamp) {
        this(null, transactionId, timestamp);
    }

    private CreateTransactionResponse(Throwable exception, UUIDTransactionId transactionId, long timestamp) {
        super(exception);
        this.transactionId = transactionId;
        this.timestamp = timestamp;
    }

    /**
     * 新创建的事务ID
     * @return 事务ID
     */
    public UUIDTransactionId getTransactionId() {
        return transactionId;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
