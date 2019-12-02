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
package io.journalkeeper.core.client;

import io.journalkeeper.base.Serializer;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.SerializedUpdateRequest;
import io.journalkeeper.core.api.UpdateRequest;
import io.journalkeeper.core.api.transaction.TransactionContext;
import io.journalkeeper.core.api.transaction.TransactionId;
import io.journalkeeper.core.api.transaction.JournalKeeperTransactionContext;
import io.journalkeeper.core.api.transaction.UUIDTransactionId;
import io.journalkeeper.rpc.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * 客户端实现
 * @author LiYue
 * Date: 2019-03-25
 */
public class DefaultRaftClient<E, ER, Q, QR> extends AbstractClient implements RaftClient<E, ER, Q, QR> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRaftClient.class);
    private final Serializer<E> entrySerializer;
    private final Serializer<ER> entryResultSerializer;
    private final Serializer<Q> querySerializer;
    private final Serializer<QR> resultSerializer;

    public DefaultRaftClient(ClientRpc clientRpc,
                             Serializer<E> entrySerializer,
                             Serializer<ER> entryResultSerializer,
                             Serializer<Q> querySerializer,
                             Serializer<QR> queryResultSerializer,
                             Properties properties) {
        super(clientRpc);
        this.entrySerializer = entrySerializer;
        this.entryResultSerializer = entryResultSerializer;
        this.querySerializer = querySerializer;
        this.resultSerializer = queryResultSerializer;

    }

    @Override
    public CompletableFuture<List<ER>> update(List<UpdateRequest<E>> entries, boolean includeHeader, ResponseConfig responseConfig) {
        return update(
                entries.stream().map(r -> new SerializedUpdateRequest(r, entrySerializer)).collect(Collectors.toList()),
                includeHeader, responseConfig, entryResultSerializer);
    }


    @Override
    public CompletableFuture<QR> query(Q query) {
        return clientRpc.invokeClientLeaderRpc(leaderRpc -> leaderRpc.queryClusterState(new QueryStateRequest(querySerializer.serialize(query))))
                .thenApply(super::checkResponse)
                .thenApply(QueryStateResponse::getResult)
                .thenApply(resultSerializer::parse);
    }

    @Override
    public CompletableFuture<TransactionContext> createTransaction(Map<String, String> context) {
        return clientRpc.invokeClientLeaderRpc(rpc -> rpc.createTransaction(new CreateTransactionRequest(context)))
                .thenApply(super::checkResponse)
                .thenApply(response -> new JournalKeeperTransactionContext(
                        response.getTransactionId(),
                        context,
                        response.getTimestamp()
                ));
    }

    @Override
    public CompletableFuture<Void> completeTransaction(TransactionId transactionId, boolean commitOrAbort) {
        return clientRpc.invokeClientLeaderRpc(leaderRpc -> leaderRpc.completeTransaction(
                new CompleteTransactionRequest(((UUIDTransactionId) transactionId).getUuid(), commitOrAbort)))
                .thenApply(super::checkResponse)
                .thenApply(response -> null);
    }

    @Override
    public CompletableFuture<Collection<TransactionContext>> getOpeningTransactions() {
        return clientRpc.invokeClientLeaderRpc(ClientServerRpc::getOpeningTransactions)
                .thenApply(super::checkResponse)
                .thenApply(GetOpeningTransactionsResponse::getTransactionContexts)
                .thenApply(ctxs -> new ArrayList<>(ctxs));
    }

    @Override
    public CompletableFuture<Void> update(TransactionId transactionId, List<UpdateRequest<E>> entries, boolean includeHeader) {
        return
                clientRpc.invokeClientLeaderRpc(rpc -> rpc.updateClusterState(new UpdateClusterStateRequest(
                        ((UUIDTransactionId) transactionId).getUuid(),
                        entries.stream().map(r -> new SerializedUpdateRequest(r, entrySerializer)).collect(Collectors.toList()),
                        includeHeader)))
                        .thenApply(this::checkResponse)
                        .thenApply(response -> null);
    }

}
