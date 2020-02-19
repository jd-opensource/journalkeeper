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
package io.journalkeeper.utils.retry;

import io.journalkeeper.utils.async.Async;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    public final <R /* Response */> CompletableFuture<R> retry(RpcInvoke<R, D> invoke, CheckRetry<? super R> checkRetry, Executor executor, ScheduledExecutorService scheduledExecutor) {
        return retry(invoke, checkRetry, null, executor, scheduledExecutor);
    }
    public final <R /* Response */> CompletableFuture<R> retry(RpcInvoke<R, D> invoke, CheckRetry<? super R> checkRetry, D fixDestination, Executor executor, ScheduledExecutorService scheduledExecutor) {

        RpcInvokeWithRetryInfo<R, D> retryInvoke = invoke instanceof CompletableRetry.RpcInvokeWithRetryInfo ? (RpcInvokeWithRetryInfo<R, D>) invoke : new RpcInvokeWithRetryInfo<>(invoke);

        CompletableFuture<D> destFuture;
        if(fixDestination == null) {
            destFuture = executor == null ?
                    CompletableFuture.completedFuture(getDestination(retryInvoke)) :
                    CompletableFuture.supplyAsync(() -> getDestination(retryInvoke), executor);
        } else {
            destFuture = CompletableFuture.completedFuture(fixDestination);
        }
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
                        long delay = retryPolicy.getRetryDelayMs(retryInvoke.getInvokeTimes());
                        logger.debug("Retry, invokes times: {}, " +
                                        "last result: {}, " +
                                        "last destination: {}, " +
                                        "next retry delay: {}ms.",
                                retryInvoke.getInvokeTimes(), r, destination.get(), delay);

                        destination.set(null);
                        if (delay > 0) {
                            return Async.scheduleAsync(scheduledExecutor, () -> retry(retryInvoke, checkRetry, fixDestination, executor, scheduledExecutor), delay, TimeUnit.MILLISECONDS);
                        } else if (delay == 0){
                            return retry(retryInvoke, checkRetry, fixDestination, executor, scheduledExecutor);
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
                invokedDestinations.add(destination);
                return rpcInvoke.invoke(destination);
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

        @Override
        public String toString() {
            return "ResultAndException{" +
                    "result=" + result +
                    ", throwable=" + throwable +
                    '}';
        }
    }
}
