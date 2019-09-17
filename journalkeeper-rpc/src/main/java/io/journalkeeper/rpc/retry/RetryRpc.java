package io.journalkeeper.rpc.retry;

import io.journalkeeper.exceptions.NotLeaderException;
import io.journalkeeper.exceptions.RequestTimeoutException;
import io.journalkeeper.exceptions.ServerBusyException;
import io.journalkeeper.exceptions.TransportException;
import io.journalkeeper.rpc.BaseResponse;
import io.journalkeeper.rpc.LeaderResponse;
import io.journalkeeper.rpc.RpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author LiYue
 * Date: 2019-09-17
 */
public class RetryRpc {
    private long minRetryDelayMs;
    private long maxRetryDelayMs;
    private int maxRetries;

    RetryRpc(long minRetryDelayMs, long maxRetryDelayMs, int maxRetries) {
        this.minRetryDelayMs = minRetryDelayMs;
        this.maxRetryDelayMs = maxRetryDelayMs;
        this.maxRetries = maxRetries;
    }

    private static final Logger logger = LoggerFactory.getLogger(RetryRpc.class);
    <O extends BaseResponse /* Response */> CompletableFuture<O> retry(RpcInvokeNoParam<O> invoke) {
        return retry( null, aVoid -> invoke.invoke());
    }

    <O extends BaseResponse /* Response */, I /* Request */> CompletableFuture<O> retry(I request, RpcInvoke<O, I> invoke) {
        RetryInvoke<O, I> retryInvoke = invoke instanceof RetryInvoke ? (RetryInvoke<O, I>) invoke: new RetryInvoke<>(invoke);
        return retryInvoke
                .invoke(request)
                .thenApply(this::checkStatusCode)
                .exceptionally(e -> {
                    try {
                        throw e instanceof CompletionException ? e.getCause() : e;
                    } catch (RequestTimeoutException | ServerBusyException | TransportException ne) {
                        logger.warn("Retry client server rpc, invokes times: {}, cause: {}", retryInvoke.getInvokeTimes(), e.getMessage());
                        try {
                            if(retryInvoke.getInvokeTimes() <= maxRetries) {
                                return retry(request, retryInvoke).get();
                            } else {
                                throw ne;
                            }
                        } catch (InterruptedException ex) {
                            throw new CompletionException(e);
                        } catch (ExecutionException ex) {
                            throw new CompletionException(e.getCause());
                        }
                    } catch (Throwable t) {
                        throw new CompletionException(t);
                    }
                });
    }

    private <O extends BaseResponse> O checkStatusCode(O response) {
        switch (response.getStatusCode()) {
            case NOT_LEADER: throw new NotLeaderException(((LeaderResponse) response).getLeader());
            case TIMEOUT: throw new RequestTimeoutException();
            case SERVER_BUSY: throw new ServerBusyException();
            case TRANSPORT_FAILED: throw new TransportException();
            case SUCCESS: return response;
            default:
                throw new RpcException(response);
        }
    }

    interface RpcInvoke<O extends BaseResponse /* Response */, I /* Request */> {
        CompletableFuture<O> invoke(I request);
    }
    interface RpcInvokeNoParam<O extends BaseResponse /* Response */> {
        CompletableFuture<O> invoke();
    }

    private  class RetryInvoke<O extends BaseResponse /* Response */, I /* Request */> implements RpcInvoke<O, I> {
        private final RpcInvoke<O, I> rpcInvoke;
        private int invokeTimes = 0;

        public RetryInvoke(RpcInvoke<O, I> rpcInvoke) {
            this.rpcInvoke = rpcInvoke;
        }


        @Override
        public CompletableFuture<O> invoke(I request) {
            if(invokeTimes++ > 0) {
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(minRetryDelayMs, maxRetryDelayMs));
                } catch (InterruptedException e) {
                    throw new CompletionException(e);
                }
            }
            return rpcInvoke.invoke(request);
        }

        public int getInvokeTimes() {
            return invokeTimes;
        }
    }

    long getMinRetryDelayMs() {
        return minRetryDelayMs;
    }

    void setMinRetryDelayMs(long minRetryDelayMs) {
        this.minRetryDelayMs = minRetryDelayMs;
    }

    long getMaxRetryDelayMs() {
        return maxRetryDelayMs;
    }

    void setMaxRetryDelayMs(long maxRetryDelayMs) {
        this.maxRetryDelayMs = maxRetryDelayMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
}
