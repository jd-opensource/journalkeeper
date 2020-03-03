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

import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.client.ClientServerRpc;
import io.journalkeeper.utils.event.EventWatcher;
import io.journalkeeper.utils.retry.CheckRetry;
import io.journalkeeper.utils.retry.CompletableRetry;
import io.journalkeeper.utils.retry.DestinationSelector;
import io.journalkeeper.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * 绑定到本地Server的Client，只访问本地Server
 * @author LiYue
 * Date: 2019/10/17
 */
public class LocalClientRpc implements ClientRpc {
    private static final Logger logger = LoggerFactory.getLogger(LocalClientRpc.class);
    private final ClientServerRpc localServer;
    private final CompletableRetry<URI> completableRetry;
    private final CheckRetry<BaseResponse> checkRetry = new LocalClientCheckRetry();
    private final ScheduledExecutorService scheduledExecutor;
    private final URI localUri;
    public LocalClientRpc(ClientServerRpc localServer, RetryPolicy retryPolicy, ScheduledExecutorService scheduledExecutor) {
        this.localServer = localServer;
        this.scheduledExecutor = scheduledExecutor;
        localUri = localServer.serverUri();
        DestinationSelector<URI> uriSelector = uriSet -> localUri;

        this.completableRetry = new CompletableRetry<>(retryPolicy, uriSelector);

    }

    @Override
    public <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke) {
        return completableRetry.retry(uri -> invoke.invoke(localServer), checkRetry, localUri, null, scheduledExecutor);
    }

    @Override
    public <O extends BaseResponse> CompletableFuture<O> invokeClientServerRpc(URI uri, CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke) {
        if (localServer.serverUri().equals(uri)) {
            return invokeClientServerRpc(invoke);
        } else {
            throw new IllegalArgumentException(
                    String.format("Request uri %s is NOT accessible!" +
                                    "You should only request to local server %s in local client mode.",
                            uri.toString(), localServer.serverUri().toString())
            );
        }
    }

    @Override
    public <O extends BaseResponse> CompletableFuture<O> invokeClientLeaderRpc(CompletableRetry.RpcInvoke<O, ClientServerRpc> invoke) {
        return invokeClientServerRpc(invoke);
    }

    @Override
    public URI getPreferredServer() {
        return localServer.serverUri();
    }

    @Override
    public void setPreferredServer(URI preferredServer) {
        // do nothing
    }

    @Override
    public void stop() {
        // do nothing
    }

    @Override
    public void updateServers(List<URI> servers) {
        // do nothing
    }

    @Override
    public void watch(EventWatcher eventWatcher) {
        localServer.watch(eventWatcher);
    }

    @Override
    public void unWatch(EventWatcher eventWatcher) {
        localServer.unWatch(eventWatcher);
    }

    private static class LocalClientCheckRetry implements CheckRetry<BaseResponse> {

        @Override
        public boolean checkException(Throwable exception) {
            try {
                logger.debug("Rpc exception: {}", exception.getMessage());
                throw exception;
            } catch (ServerBusyException | TimeoutException ignored) {
                return true;
            } catch (Throwable ignored) {
            }
            return false;
        }

        @Override
        public boolean checkResult(BaseResponse response) {
            switch (response.getStatusCode()) {
                case TIMEOUT:
                case SERVER_BUSY:
                    logger.info("{} failed, cause: {}, Retry...", response.getClass().getName(), response.errorString());
                    return true;
                default:
                    return false;
            }
        }
    }
}
