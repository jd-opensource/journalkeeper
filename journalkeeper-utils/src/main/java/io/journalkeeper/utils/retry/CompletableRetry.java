package io.journalkeeper.utils.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author LiYue
 * Date: 2019-09-17
 */
public class CompletableRetry<D/* 对端地址类型 */> {
    private final RetryPolicy retryPolicy;

    private AtomicReference<D> destination = new AtomicReference<>(null);
    private final DestinationSelector<D> destinationSelector;

    public CompletableRetry(RetryPolicy retryPolicy, DestinationSelector<D> destinationSelector) {
        this.retryPolicy = retryPolicy;
        this.destinationSelector = destinationSelector;
    }


    private static final Logger logger = LoggerFactory.getLogger(CompletableRetry.class);

    private <O> D getDestination(RpcInvokeWithRetryInfo<O, D> retryInvoke) {
        destination.compareAndSet(null, destinationSelector.select(retryInvoke.getInvokedDestinations()));
//        logger.info("Using destination: {}", destination.get());
        return destination.get();
    }

    public final <R /* Response */> CompletableFuture<R> retry(RpcInvoke<R, D> invoke, CheckRetry<? super R> checkRetry, Executor executor) {

        RpcInvokeWithRetryInfo<R, D> retryInvoke = invoke instanceof CompletableRetry.RpcInvokeWithRetryInfo ? (RpcInvokeWithRetryInfo<R, D>) invoke : new RpcInvokeWithRetryInfo<>(invoke);
        CompletableFuture<D> destFuture = executor == null ?
                CompletableFuture.completedFuture(getDestination(retryInvoke)) :
                CompletableFuture.supplyAsync(() -> getDestination(retryInvoke), executor);

        return destFuture
                .thenCompose(retryInvoke::invoke)
                .thenApply(ResultAndException::new)
                .exceptionally(ResultAndException::new)
                .thenCompose(r -> {
                    boolean retry;
                    if(null != r.getThrowable()) {
                        retry = checkRetry.checkException(r.getThrowable());
                    } else {
                        retry = checkRetry.checkResult(r.getResult());
                    }
                    if(retry) {
                        destination.set(null);
                        long delay;
                        if((delay = retryPolicy.getRetryDelayMs(retryInvoke.getInvokeTimes())) >= 0) {
                            try {
                                if (delay > 0) {
                                    Thread.sleep(delay);
                                    logger.warn("Retry, invokes times: {}.", retryInvoke.getInvokeTimes());
                                }
                                return retry(retryInvoke, checkRetry, executor);
                            } catch (InterruptedException ignored) {
                                logger.warn("Retry interrupted!");
                            }
                        }
                    }
                    CompletableFuture<R> future = new CompletableFuture<>();
                    if(r.getThrowable() != null) {
                        future.completeExceptionally(r.getThrowable());
                    } else {
                        future.complete(r.getResult());
                    }
                    return future;
                });
    }

    public interface RpcInvoke<R /* Response */,  D /* Destination */> {
        CompletableFuture<R> invoke(D destination);
    }

    private static class RpcInvokeWithRetryInfo<R /* Response */,  D /* Destination */> implements RpcInvoke<R, D> {
        private final RpcInvoke<R, D> rpcInvoke;
        private int invokeTimes = 0;
        private final Set<D> invokedDestinations = new HashSet<>();

        public RpcInvokeWithRetryInfo(RpcInvoke<R, D> rpcInvoke) {
            this.rpcInvoke = rpcInvoke;
        }


        @Override
        public CompletableFuture<R> invoke(D destination) {
            try {
                invokeTimes++;
                CompletableFuture<R> future = rpcInvoke.invoke( destination);
                invokedDestinations.add(destination);
                return future;
            } catch (Throwable throwable) {
                CompletableFuture<R> future = new CompletableFuture<>();
                future.completeExceptionally(throwable);
                return future;
            }
        }

        public int getInvokeTimes() {
            return invokeTimes;
        }

        public Set<D> getInvokedDestinations() {
            return invokedDestinations;
        }
    }

    private static class ResultAndException<R> {
        ResultAndException(R result) {
            this.result = result;
            this.throwable = null;
        }

        ResultAndException(Throwable e) {
            this.result = null;
            this.throwable = getCause(e);
        }
        private final R result;
        private final Throwable throwable;

        public R getResult() {
            return result;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        private Throwable getCause(Throwable e) {
            if((e instanceof CompletionException || e instanceof ExecutionException) && null != e.getCause()) {
                return getCause(e.getCause());
            } else {
                return e;
            }
        }
    }
}
