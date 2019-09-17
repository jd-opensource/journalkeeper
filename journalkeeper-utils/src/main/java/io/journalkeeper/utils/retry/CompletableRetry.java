package io.journalkeeper.utils.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author LiYue
 * Date: 2019-09-17
 */
public class CompletableRetry<D/* 对端地址类型 */> {
    private long minRetryDelayMs;
    private long maxRetryDelayMs;
    private int maxRetries;

    private AtomicReference<D> destination = new AtomicReference<>(null);
    private final DestinationSelector<D> destinationSelector;

    public CompletableRetry(long minRetryDelayMs, long maxRetryDelayMs, int maxRetries, DestinationSelector<D> destinationSelector) {
        this.minRetryDelayMs = minRetryDelayMs;
        this.maxRetryDelayMs = maxRetryDelayMs;
        this.maxRetries = maxRetries;
        this.destinationSelector = destinationSelector;
    }


    private static final Logger logger = LoggerFactory.getLogger(CompletableRetry.class);

    private <O, I> D getDestination(RpcInvokeWithRetryInfo<O, I, D> retryInvoke) {
        destination.compareAndSet(null, destinationSelector.select(retryInvoke.getInvokedDestinations()));
        return destination.get();
    }
    public <O /* Response */> CompletableFuture<O> retry(RpcInvokeNoRequest<O, D> invoke, CheckRetry<? super O> checkRetry) {
        return retry(invoke, checkRetry, null);
    }

    public <O /* Response */> CompletableFuture<O> retry(RpcInvokeNoRequest<O, D> invoke, CheckRetry<? super O> checkRetry, Executor executor) {
        return retry(null, (request, destination) -> invoke.invoke(destination), checkRetry, executor);
    }

    public <O /* Response */, I /* Request */> CompletableFuture<O> retry(I request, RpcInvoke<O, I, D> invoke, CheckRetry<? super O> checkRetry) {
        return retry(request, invoke, checkRetry, null);
    }

    public <O /* Response */, I /* Request */> CompletableFuture<O> retry(I request, RpcInvoke<O, I, D> invoke, CheckRetry<? super O> checkRetry, Executor executor) {

        RpcInvokeWithRetryInfo<O, I, D> retryInvoke = invoke instanceof CompletableRetry.RpcInvokeWithRetryInfo ? (RpcInvokeWithRetryInfo<O, I, D>) invoke : new RpcInvokeWithRetryInfo<>(invoke);
        CompletableFuture<D> destFuture = executor == null ?
                CompletableFuture.completedFuture(getDestination(retryInvoke)) :
                CompletableFuture.supplyAsync(() -> {
                    destination.compareAndSet(null, destinationSelector.select(retryInvoke.getInvokedDestinations()));
                    return destination.get();
                }, executor);

        return destFuture
                .thenCompose(destination -> retryInvoke.invoke(request, destination))
                .thenApply(ResultAndException::new)
                .exceptionally(ResultAndException::new)
                .thenCompose(r -> {
                    boolean retry;
                    if(null != r.getThrowable()) {
                        retry = checkRetry.checkException(r.getThrowable());
                    } else {
                        retry = checkRetry.checkResult(r.getResult());
                    }
                    if(retry && retryInvoke.getInvokeTimes() <= maxRetries) {
                        logger.warn("Retry, invokes times: {}.", retryInvoke.getInvokeTimes());
                        return retry(request, retryInvoke, checkRetry, executor);
                    } else {
                        return CompletableFuture.completedFuture(r.getResult());
                    }
                });
    }

    public interface RpcInvoke<O /* Response */, I /* Request */, D /* Destination */> {
        CompletableFuture<O> invoke(I request, D destination);
    }

    public interface RpcInvokeNoRequest<O /* Response */,  D /* Destination */> {
        CompletableFuture<O> invoke( D destination);
    }

    private class RpcInvokeWithRetryInfo<O /* Response */, I /* Request */, D /* Destination */> implements RpcInvoke<O, I, D> {
        private final RpcInvoke<O, I, D> rpcInvoke;
        private int invokeTimes = 0;
        private final Set<D> invokedDestinations = new HashSet<>(maxRetries);

        public RpcInvokeWithRetryInfo(RpcInvoke<O, I, D> rpcInvoke) {
            this.rpcInvoke = rpcInvoke;
        }


        @Override
        public CompletableFuture<O> invoke(I request, D destination) {
            if (invokeTimes++ > 0) {
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(minRetryDelayMs, maxRetryDelayMs));
                } catch (InterruptedException e) {
                    throw new CompletionException(e);
                }
            }

            CompletableFuture<O> future = rpcInvoke.invoke(request, destination);
            invokedDestinations.add(destination);
            return future;
        }

        public int getInvokeTimes() {
            return invokeTimes;
        }

        public Set<D> getInvokedDestinations() {
            return invokedDestinations;
        }
    }

    public long getMinRetryDelayMs() {
        return minRetryDelayMs;
    }

    public void setMinRetryDelayMs(long minRetryDelayMs) {
        this.minRetryDelayMs = minRetryDelayMs;
    }

    public long getMaxRetryDelayMs() {
        return maxRetryDelayMs;
    }

    public void setMaxRetryDelayMs(long maxRetryDelayMs) {
        this.maxRetryDelayMs = maxRetryDelayMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    private static class ResultAndException<R> {
        ResultAndException(R result) {
            this.result = result;
            this.throwable = null;
        }

        ResultAndException(Throwable e) {
            this.result = null;
            this.throwable = e;
        }
        private final R result;
        private final Throwable throwable;

        public R getResult() {
            return result;
        }

        public Throwable getThrowable() {
            return throwable;
        }

    }
}
