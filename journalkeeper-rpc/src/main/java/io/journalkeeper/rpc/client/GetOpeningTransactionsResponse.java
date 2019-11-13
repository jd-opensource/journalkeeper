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
