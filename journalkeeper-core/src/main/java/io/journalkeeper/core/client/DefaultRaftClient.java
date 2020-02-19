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
package io.journalkeeper.core.client;

import io.journalkeeper.core.api.QueryConsistency;
import io.journalkeeper.core.api.RaftClient;
import io.journalkeeper.core.api.ResponseConfig;
import io.journalkeeper.core.api.UpdateRequest;
import io.journalkeeper.core.api.transaction.JournalKeeperTransactionContext;
import io.journalkeeper.core.api.transaction.TransactionContext;
import io.journalkeeper.core.api.transaction.TransactionId;
import io.journalkeeper.core.api.transaction.UUIDTransactionId;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.rpc.client.CompleteTransactionRequest;
import io.journalkeeper.rpc.client.CreateTransactionRequest;
import io.journalkeeper.rpc.client.GetOpeningTransactionsResponse;
import io.journalkeeper.rpc.client.QueryStateRequest;
import io.journalkeeper.rpc.client.QueryStateResponse;
import io.journalkeeper.rpc.client.UpdateClusterStateRequest;
import io.journalkeeper.rpc.client.UpdateClusterStateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 客户端实现
 * @author LiYue
 * Date: 2019-03-25
 */
public class DefaultRaftClient extends AbstractClient implements RaftClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRaftClient.class);
    private final AtomicLong lastApplied = new AtomicLong(-1L);
    public DefaultRaftClient(ClientRpc clientRpc,
                             Properties properties) {
        super(clientRpc);
    }

    @Override
    public CompletableFuture<List<byte[]>> update(List<UpdateRequest> entries, boolean includeHeader, ResponseConfig responseConfig) {
        return
                clientRpc.invokeClientLeaderRpc(rpc -> rpc.updateClusterState(new UpdateClusterStateRequest(entries, includeHeader, responseConfig)))
                        .thenApply(this::checkResponse)
                        .thenApply(response -> {
                            maybeUpdateLastApplied(response.getLastApplied());
                            return response;
                        })
                        .thenApply(UpdateClusterStateResponse::getResults);
    }


    @Override
    public CompletableFuture<byte[]> query(byte[] query, QueryConsistency consistency) {
        if (consistency == QueryConsistency.STRICT) {
            return query(query);
        }
        return queryAllServers(query, consistency);
    }

    private CompletableFuture<byte[]> queryAllServers(byte[] query, QueryConsistency consistency) {
       return clientRpc.invokeClientServerRpc(
               rpc -> rpc.queryServerState(new QueryStateRequest(query, consistency == QueryConsistency.NONE ? -1L : lastApplied.get()))
       )
                .thenApply(super::checkResponse)
                .thenApply(response -> {
                    maybeUpdateLastApplied(response.getLastApplied());
                    return response;
                })
                .thenApply(QueryStateResponse::getResult);

    }

    @Override
    public CompletableFuture<byte[]> query(byte[] query) {

        return clientRpc.invokeClientLeaderRpc(leaderRpc -> leaderRpc.queryClusterState(new QueryStateRequest(query)))
                .thenApply(super::checkResponse)
                .thenApply(response -> {
                    maybeUpdateLastApplied(response.getLastApplied());
                    return response;
                })
                .thenApply(QueryStateResponse::getResult);
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
                .thenApply(contexts -> contexts.stream().map(c -> (TransactionContext) c).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> update(TransactionId transactionId, List<UpdateRequest> entries, boolean includeHeader) {
        return
                clientRpc.invokeClientLeaderRpc(rpc -> rpc.updateClusterState(new UpdateClusterStateRequest(
                        ((UUIDTransactionId) transactionId).getUuid(),
                        entries,
                        includeHeader)))
                        .thenApply(this::checkResponse)
                        .thenApply(response -> {
                            maybeUpdateLastApplied(response.getLastApplied());
                            return null;
                        });
    }

    private void maybeUpdateLastApplied(long index) {
        for (;;) {
            long finalLastApplied = lastApplied.get();
            if (index > finalLastApplied) {
                if(lastApplied.compareAndSet(finalLastApplied, index)) {
                    return;
                };
            } else {
                return;
            }
        }
    }

}
