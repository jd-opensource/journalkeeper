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
package io.journalkeeper.rpc.handler;

import io.journalkeeper.rpc.remoting.transport.command.support.DefaultCommandHandlerFactory;
import io.journalkeeper.rpc.server.ServerRpc;

/**
 * @author LiYue
 * Date: 2019-04-02
 */
public class ServerRpcCommandHandlerRegistry {
    public static void register(DefaultCommandHandlerFactory factory, ServerRpc serverRpc) {
        factory.register(new UpdateClusterStateHandler(serverRpc));
        factory.register(new LastAppliedHandler(serverRpc));
        factory.register(new QueryClusterStateHandler(serverRpc));
        factory.register(new QueryServerStateHandler(serverRpc));
        factory.register(new QuerySnapshotHandler(serverRpc));
        factory.register(new GetServersHandler(serverRpc));
        factory.register(new AddPullWatchHandler(serverRpc));
        factory.register(new RemovePullWatchHandler(serverRpc));
        factory.register(new PullEventsHandler(serverRpc));
        factory.register(new UpdateVotersHandler(serverRpc));
        factory.register(new ConvertRollHandler(serverRpc));

        factory.register(new AsyncAppendEntriesHandler(serverRpc));
        factory.register(new RequestVoteHandler(serverRpc));
        factory.register(new GetServerEntriesHandler(serverRpc));
        factory.register(new GetServerStateHandler(serverRpc));

    }
}
