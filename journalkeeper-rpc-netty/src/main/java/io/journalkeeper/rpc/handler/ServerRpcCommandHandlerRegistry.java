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
package io.journalkeeper.rpc.handler;

import io.journalkeeper.rpc.remoting.transport.command.support.UriRoutedCommandHandlerFactory;
import io.journalkeeper.rpc.server.ServerRpc;

import java.net.URI;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class ServerRpcCommandHandlerRegistry {
    public static void register(UriRoutedCommandHandlerFactory factory, ServerRpc serverRpc) {
        URI uri = serverRpc.serverUri();
        factory.register(uri, new UpdateClusterStateHandler(serverRpc));
        factory.register(uri, new LastAppliedHandler(serverRpc));
        factory.register(uri, new QueryClusterStateHandler(serverRpc));
        factory.register(uri, new QueryServerStateHandler(serverRpc));
        factory.register(uri, new QuerySnapshotHandler(serverRpc));
        factory.register(uri, new GetServersHandler(serverRpc));
        factory.register(uri, new AddPullWatchHandler(serverRpc));
        factory.register(uri, new RemovePullWatchHandler(serverRpc));
        factory.register(uri, new PullEventsHandler(serverRpc));
        factory.register(uri, new UpdateVotersHandler(serverRpc));
        factory.register(uri, new ConvertRollHandler(serverRpc));
        factory.register(uri, new GetServerStatusHandler(serverRpc));
        factory.register(uri, new CreateTransactionHandler(serverRpc));
        factory.register(uri, new GetOpeningTransactionsHandler(serverRpc));
        factory.register(uri, new CompleteTransactionHandler(serverRpc));
        factory.register(uri, new GetSnapshotsHandler(serverRpc));
        factory.register(uri, new CheckLeadershipHandler(serverRpc));

        factory.register(uri, new AsyncAppendEntriesHandler(serverRpc));
        factory.register(uri, new RequestVoteHandler(serverRpc));
        factory.register(uri, new GetServerEntriesHandler(serverRpc));
        factory.register(uri, new GetServerStateHandler(serverRpc));
        factory.register(uri, new DisableLeaderWriteRequestHandler(serverRpc));
        factory.register(uri, new InstallSnapshotHandler(serverRpc));

    }
}
